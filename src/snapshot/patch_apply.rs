//! Defines the algorithm for applying generated patches.

use std::{
    collections::{HashMap, HashSet},
    mem::take,
};

use rbx_dom_weak::types::{Ref, Variant};

use super::{
    patch::{AppliedPatchSet, AppliedPatchUpdate, PatchSet, PatchUpdate},
    InstanceSnapshot, RojoTree,
};

/// Consumes the input `PatchSet`, applying all of its prescribed changes to the
/// tree and returns an `AppliedPatchSet`, which can be used to keep another
/// tree in sync with Rojo's.
#[profiling::function]
pub fn apply_patch_set(tree: &mut RojoTree, patch_set: PatchSet) -> AppliedPatchSet {
    let mut context = PatchApplyContext::new(patch_set.client_id);

    {
        profiling::scope!("removals");
        for removed_id in patch_set.removed_instances {
            apply_remove_instance(&mut context, tree, removed_id);
        }
    }

    {
        profiling::scope!("additions");
        for add_patch in patch_set.added_instances {
            apply_add_child(&mut context, tree, add_patch.parent_id, add_patch.instance);
        }
    }

    {
        profiling::scope!("updates");
        // Updates need to be applied after additions, which reduces the complexity
        // of updates significantly.
        for update_patch in patch_set.updated_instances {
            apply_update_child(&mut context, tree, update_patch);
        }
    }

    finalize_patch_application(context, tree)
}

/// All of the ephemeral state needing during application of a patch.
#[derive(Default)]
struct PatchApplyContext {
    /// A map from transient snapshot IDs (generated by snapshot middleware) to
    /// instance IDs in the actual tree. These are both the same data type so
    /// that they fit into the same `Variant::Ref` type.
    ///
    /// At this point in the patch process, IDs in instance properties have been
    /// partially translated from 'snapshot space' into 'tree space' by the
    /// patch computation process. An ID not existing in this map means either:
    ///
    /// 1. The ID is already in tree space and refers to an instance that
    ///    existed in the tree before this patch was applied.
    ///
    /// 2. The ID if in snapshot space, but points to an instance that was not
    ///    part of the snapshot that was put through the patch computation
    ///    function.
    ///
    /// #2 should not occur in well-formed projects, but is indistinguishable
    /// from #1 right now. It could happen if two model files try to reference
    /// eachother.
    snapshot_id_to_instance_id: HashMap<Ref, Ref>,

    /// Tracks all of the instances added by this patch that have refs that need
    /// to be rewritten.
    has_refs_to_rewrite: HashSet<Ref>,

    /// The current applied patch result, describing changes made to the tree.
    applied_patch_set: AppliedPatchSet,
}

impl PatchApplyContext {
    fn new(client_id: Option<String>) -> Self {
        Self {
            snapshot_id_to_instance_id: HashMap::new(),
            has_refs_to_rewrite: HashSet::new(),
            applied_patch_set: AppliedPatchSet::new(client_id),
        }
    }
}

/// Finalize this patch application, consuming the context, applying any
/// deferred property updates, and returning the finally applied patch set.
///
/// Ref properties from snapshots refer to eachother via snapshot ID. Some of
/// these properties are transformed when the patch is computed, notably the
/// instances that the patch computing method is able to pair up.
///
/// The remaining Ref properties need to be handled during patch application,
/// where we build up a map of snapshot IDs to instance IDs as they're created,
/// then apply properties all at once at the end.
#[profiling::function]
fn finalize_patch_application(context: PatchApplyContext, tree: &mut RojoTree) -> AppliedPatchSet {
    for id in context.has_refs_to_rewrite {
        // This should always succeed since instances marked as added in our
        // patch should be added without fail.
        let mut instance = tree
            .get_instance_mut(id)
            .expect("Invalid instance ID in deferred property map");

        for value in instance.properties_mut().values_mut() {
            if let Variant::Ref(referent) = value {
                if let Some(&instance_referent) = context.snapshot_id_to_instance_id.get(&referent)
                {
                    *value = Variant::Ref(instance_referent);
                }
            }
        }
    }

    context.applied_patch_set
}

fn apply_remove_instance(context: &mut PatchApplyContext, tree: &mut RojoTree, removed_id: Ref) {
    tree.remove(removed_id);
    context.applied_patch_set.removed.push(removed_id);
}

fn apply_add_child(
    context: &mut PatchApplyContext,
    tree: &mut RojoTree,
    parent_id: Ref,
    mut snapshot: InstanceSnapshot,
) {
    let snapshot_id = snapshot.snapshot_id;
    let children = take(&mut snapshot.children);

    // If an object we're adding has a non-null referent, we'll note this
    // instance down as needing to be revisited later.
    let has_refs = snapshot.properties.values().any(|value| match value {
        Variant::Ref(value) => value.is_some(),
        _ => false,
    });

    let id = tree.insert_instance(parent_id, snapshot);
    context.applied_patch_set.added.push(id);

    if has_refs {
        context.has_refs_to_rewrite.insert(id);
    }

    if let Some(snapshot_id) = snapshot_id {
        context.snapshot_id_to_instance_id.insert(snapshot_id, id);
    }

    for child in children {
        apply_add_child(context, tree, id, child);
    }
}

fn apply_update_child(context: &mut PatchApplyContext, tree: &mut RojoTree, patch: PatchUpdate) {
    let mut applied_patch = AppliedPatchUpdate::new(patch.id);

    if let Some(metadata) = patch.changed_metadata {
        tree.update_metadata(patch.id, metadata.clone());
        applied_patch.changed_metadata = Some(metadata);
    }

    let mut instance = match tree.get_instance_mut(patch.id) {
        Some(instance) => instance,
        None => {
            log::warn!(
                "Patch misapplication: Instance {:?}, referred to by update patch, did not exist.",
                patch.id
            );
            return;
        }
    };

    if let Some(name) = patch.changed_name {
        *instance.name_mut() = name.clone();
        applied_patch.changed_name = Some(name);
    }

    if let Some(class_name) = patch.changed_class_name {
        *instance.class_name_mut() = class_name.clone();
        applied_patch.changed_class_name = Some(class_name);
    }

    for (key, property_entry) in patch.changed_properties {
        match property_entry {
            // Ref values need to be potentially rewritten from snapshot IDs to
            // instance IDs if they referred to an instance that was created as
            // part of this patch.
            Some(Variant::Ref(referent)) => {
                if referent.is_none() {
                    continue;
                }

                // If our ID is not found in this map, then it either refers to
                // an existing instance NOT added by this patch, or there was an
                // error. See `PatchApplyContext::snapshot_id_to_instance_id`
                // for more info.
                let new_referent = context
                    .snapshot_id_to_instance_id
                    .get(&referent)
                    .copied()
                    .unwrap_or(referent);

                instance
                    .properties_mut()
                    .insert(key.clone(), Variant::Ref(new_referent));
            }
            Some(ref value) => {
                instance.properties_mut().insert(key.clone(), value.clone());
            }
            None => {
                instance.properties_mut().remove(&key);
            }
        }

        applied_patch.changed_properties.insert(key, property_entry);
    }

    context.applied_patch_set.updated.push(applied_patch)
}

#[cfg(test)]
mod test {
    use super::*;

    use std::borrow::Cow;

    use maplit::hashmap;
    use rbx_dom_weak::types::Variant;

    use super::super::PatchAdd;

    #[test]
    fn add_from_empty() {
        let _ = env_logger::try_init();

        let mut tree = RojoTree::new(InstanceSnapshot::new());

        let root_id = tree.get_root_id();

        let snapshot = InstanceSnapshot {
            snapshot_id: None,
            metadata: Default::default(),
            name: Cow::Borrowed("Foo"),
            class_name: Cow::Borrowed("Bar"),
            properties: hashmap! {
                "Baz".to_owned() => Variant::Int32(5),
            },
            children: Vec::new(),
        };

        let patch_set = PatchSet {
            added_instances: vec![PatchAdd {
                parent_id: root_id,
                instance: snapshot.clone(),
            }],
            ..Default::default()
        };

        apply_patch_set(&mut tree, patch_set);

        let root_instance = tree.get_instance(root_id).unwrap();
        let child_id = root_instance.children()[0];
        let child_instance = tree.get_instance(child_id).unwrap();

        assert_eq!(child_instance.name(), &snapshot.name);
        assert_eq!(child_instance.class_name(), &snapshot.class_name);
        assert_eq!(child_instance.properties(), &snapshot.properties);
        assert!(child_instance.children().is_empty());
    }

    #[test]
    fn update_existing() {
        let _ = env_logger::try_init();

        let mut tree = RojoTree::new(
            InstanceSnapshot::new()
                .class_name("OldClassName")
                .name("OldName")
                .property("Foo", 7i32)
                .property("Bar", 3i32)
                .property("Unchanged", -5i32),
        );

        let root_id = tree.get_root_id();

        let patch = PatchUpdate {
            id: root_id,
            changed_name: Some("Foo".to_owned()),
            changed_class_name: Some("NewClassName".to_owned()),
            changed_properties: hashmap! {
                // The value of Foo has changed
                "Foo".to_owned() => Some(Variant::Int32(8)),

                // Bar has been deleted
                "Bar".to_owned() => None,

                // Baz has been added
                "Baz".to_owned() => Some(Variant::Int32(10)),
            },
            changed_metadata: None,
        };

        let patch_set = PatchSet {
            updated_instances: vec![patch],
            ..Default::default()
        };

        apply_patch_set(&mut tree, patch_set);

        let expected_properties = hashmap! {
            "Foo".to_owned() => Variant::Int32(8),
            "Baz".to_owned() => Variant::Int32(10),
            "Unchanged".to_owned() => Variant::Int32(-5),
        };

        let root_instance = tree.get_instance(root_id).unwrap();
        assert_eq!(root_instance.name(), "Foo");
        assert_eq!(root_instance.class_name(), "NewClassName");
        assert_eq!(root_instance.properties(), &expected_properties);
    }
}
