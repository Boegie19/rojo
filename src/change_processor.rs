use crossbeam_channel::{select, Receiver, RecvError, Sender};
use fs_err::File;
use jod_thread::JoinHandle;
use memofs::{IoResultExt, Vfs, VfsEvent};
use rbx_dom_weak::types::{Ref, Variant};
use std::{
    fs,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    change_bypass::ChangeBypass,
    message_queue::MessageQueue,
    resolution::UnresolvedValue,
    snapshot::{
        apply_patch_set, compute_patch_set, AppliedPatchSet, GenerationMap, InstigatingSource,
        PatchSet, RojoTree,
    },
    snapshot_middleware::{
        meta_file::{AdjacentMetadata, DirectoryMetadata},
        snapshot_from_vfs, snapshot_project_node,
        util::PathExt,
    },
    ProjectNode,
};

/// Processes file change events, updates the DOM, and sends those updates
/// through a channel for other stuff to consume.
///
/// Owns the connection between Rojo's VFS and its DOM by holding onto another
/// thread that processes messages.
///
/// Consumers of ChangeProcessor, like ServeSession, are intended to communicate
/// with this object via channels.
///
/// ChangeProcessor expects to be the only writer to the RojoTree and Vfs
/// objects passed to it.
pub struct ChangeProcessor {
    /// Controls the runtime of the processor thread. When signaled, the job
    /// thread will finish its current work and terminate.
    ///
    /// This channel should be signaled before dropping ChangeProcessor or we'll
    /// hang forever waiting for the message processing loop to terminate.
    shutdown_sender: Sender<()>,

    /// A handle to the message processing thread. When dropped, we'll block
    /// until it's done.
    ///
    /// Allowed to be unused because dropping this value has side effects.
    #[allow(unused)]
    job_thread: JoinHandle<Result<(), RecvError>>,
}

impl ChangeProcessor {
    /// Spin up the ChangeProcessor, connecting it to the given tree, VFS, and
    /// outbound message queue.
    pub fn start(
        tree: Arc<Mutex<RojoTree>>,
        vfs: Arc<Vfs>,
        message_queue: Arc<MessageQueue<AppliedPatchSet>>,
        generation_map: Arc<Mutex<GenerationMap>>,
        tree_mutation_receiver: Receiver<PatchSet>,
    ) -> Self {
        let (shutdown_sender, shutdown_receiver) = crossbeam_channel::bounded(1);
        let vfs_receiver = vfs.event_receiver();
        let task = JobThreadContext {
            tree,
            generation_map,
            vfs,
            message_queue,
            change_bypass: Arc::new(Mutex::new(ChangeBypass::new())),
        };

        let job_thread = jod_thread::Builder::new()
            .name("ChangeProcessor thread".to_owned())
            .spawn(move || {
                log::trace!("ChangeProcessor thread started");

                loop {
                    select! {
                        recv(vfs_receiver) -> event => {
                            task.handle_vfs_event(event?);
                        },
                        recv(tree_mutation_receiver) -> patch_set => {
                            task.handle_tree_event(patch_set?);
                        },
                        recv(shutdown_receiver) -> _ => {
                            log::trace!("ChangeProcessor shutdown signal received...");
                            return Ok(());
                        },
                    }
                }
            })
            .expect("Could not start ChangeProcessor thread");

        Self {
            shutdown_sender,
            job_thread,
        }
    }
}

impl Drop for ChangeProcessor {
    fn drop(&mut self) {
        // Signal the job thread to start spinning down. Without this we'll hang
        // forever waiting for the thread to finish its infinite loop.
        let _ = self.shutdown_sender.send(());

        // After this function ends, the job thread will be joined. It might
        // block for a small period of time while it processes its last work.
    }
}

/// Contains all of the state needed to synchronize the DOM and VFS.
struct JobThreadContext {
    /// A handle to the DOM we're managing.
    tree: Arc<Mutex<RojoTree>>,

    generation_map: Arc<Mutex<GenerationMap>>,

    /// A handle to the VFS we're managing.
    vfs: Arc<Vfs>,

    /// Whenever changes are applied to the DOM, we should push those changes
    /// into this message queue to inform any connected clients.
    message_queue: Arc<MessageQueue<AppliedPatchSet>>,

    change_bypass: Arc<Mutex<ChangeBypass>>,
}

impl JobThreadContext {
    fn handle_vfs_event(&self, event: VfsEvent) {
        log::trace!("Vfs event: {:?}", event);

        // Update the VFS immediately with the event.
        self.vfs
            .commit_event(&event)
            .expect("Error applying VFS change");

        // For a given VFS event, we might have many changes to different parts
        // of the tree. Calculate and apply all of these changes.
        let applied_patches = match event {
            VfsEvent::Write(path) => {
                if path.is_dir() {
                    return;
                }
                on_vfs_event(
                    path,
                    &self.tree,
                    &self.vfs,
                    &self.generation_map,
                    &self.change_bypass,
                )
            }
            VfsEvent::Create(path) | VfsEvent::Remove(path) => on_vfs_event(
                path,
                &self.tree,
                &self.vfs,
                &self.generation_map,
                &self.change_bypass,
            ),
            _ => {
                log::warn!("Unhandled VFS event: {:?}", event);
                Vec::new()
            }
        };

        // Notify anyone listening to the message queue about the changes we
        // just made.
        self.message_queue.push_messages(&applied_patches);
    }

    fn handle_tree_event(&self, patch_set: PatchSet) {
        let mut change_bypass = self.change_bypass.lock().unwrap();
        let mut tree = self.tree.lock().unwrap();
        let mut generation_map = self.generation_map.lock().unwrap();
        let mut used_paths = Vec::new();
        log::trace!("Applying PatchSet from client: {:#?}", patch_set);
        let mut rbxm_files_to_update = Vec::new();
        let applied_patch = {
            for &id in &patch_set.removed_instances {
                if let Some(instance) = tree.get_instance(id.clone()) {
                    if let Some(instigating_source) = &instance.metadata().instigating_source {
                        match instigating_source {
                            InstigatingSource::Path(path) => {
                                used_paths.push(path.clone());
                                change_bypass.insert(path.clone());
                                fs::remove_file(path).unwrap();
                            }
                            InstigatingSource::ProjectNode(_, _, _, _, _) => {
                                log::warn!(
                                    "Cannot remove instance {:?}, it's from a project file",
                                    id
                                );
                            }
                        }
                    } else {
                        let mut current_instance = instance;
                        let instigating_source = loop {
                            if let Some(instigating_source) =
                                &current_instance.metadata().instigating_source
                            {
                                break instigating_source;
                            }
                            current_instance =
                                tree.get_instance(current_instance.parent()).unwrap();
                        };
                        match instigating_source {
                            InstigatingSource::Path(path) => {
                                if let Some(extension) = path.extension() {
                                    if extension == "rbxm" || extension == "rbxmx" {
                                        if !rbxm_files_to_update
                                            .contains(&(current_instance.id(), path.clone()))
                                        {
                                            used_paths.push(path.clone());
                                            change_bypass.insert(path.to_path_buf());
                                            rbxm_files_to_update
                                                .push((current_instance.id(), path.clone()));
                                            continue;
                                        }
                                    }
                                }
                            }
                            InstigatingSource::ProjectNode(_, _, _, _, _) => {}
                        }
                        // TODO
                        log::warn!(
                            "Cannot remove instance {:?}, it is not an instigating source.",
                            id
                        );
                    }
                } else {
                    log::warn!("Cannot remove instance {:?}, it does not exist.", id);
                }
            }
            for add in &patch_set.added_instances {
                let mut instance = tree.get_instance(add.parent_id).unwrap();
                let instigating_source = loop {
                    if let Some(instigating_source) = &instance.metadata().instigating_source {
                        break instigating_source;
                    }
                    instance = tree.get_instance(instance.parent()).unwrap();
                };
                match instigating_source {
                    InstigatingSource::Path(path) => {
                        if let Some(extension) = path.extension() {
                            if extension == "rbxm" || extension == "rbxmx" {
                                if !rbxm_files_to_update.contains(&(instance.id(), path.clone())) {
                                    used_paths.push(path.clone());
                                    change_bypass.insert(path.to_path_buf());
                                    rbxm_files_to_update.push((instance.id(), path.clone()))
                                }
                            }
                        }
                    }
                    InstigatingSource::ProjectNode(_, _, _, _, _) => {
                        log::warn!(
                            "Cannot add child to {:?}, it's from a project file",
                            add.parent_id
                        );
                    }
                }
            }

            for update in &patch_set.updated_instances {
                let id = update.id;

                if update.changed_name.is_some() {
                    'changed_name: for (new_name) in &update.changed_name {

                    let instance = tree.get_instance(id).unwrap();
                    let instigating_source = loop {
                        if let Some(instigating_source) = &instance.metadata().instigating_source {
                            break instigating_source;
                        }
                        instance = tree.get_instance(instance.parent()).unwrap();
                    };
                    for path in &instance.metadata().relevant_paths {
                        if path.file_name_ends_with(".rbxm")
                            || path.file_name_ends_with(".rbxmx")
                        {
                            if id == instance.id() {
                                log::warn!("Cannot change Name of top level rbxm/rbxmx instances.");
                                continue 'changed_name;
                            }
                            if !rbxm_files_to_update.contains(&(instance.id(), path.clone())) {
                                used_paths.push(path.clone());
                                change_bypass.insert(path.to_path_buf());
                                rbxm_files_to_update.push((instance.id(), path.clone()));
                            }
                            continue 'changed_name;
                        }else{
                            log::warn!("Cannot change Name of none rbxm/rbxmx object.");
                        }
                }

                if update.changed_class_name.is_some() {
                    log::warn!("Cannot change ClassName yet.");
                }

                if update.changed_metadata.is_some() {
                    log::warn!("Cannot change metadata yet.");
                }

                if let Some(mut instance) = tree.get_instance(id) {
                    let instigating_source = loop {
                        if let Some(instigating_source) = &instance.metadata().instigating_source {
                            break instigating_source;
                        }
                        instance = tree.get_instance(instance.parent()).unwrap();
                    };
                    'changed_properties: for (key, changed_value) in &update.changed_properties {
                        if let Some(changed_value) = changed_value {
                            match instigating_source.to_owned() {
                                InstigatingSource::Path(_) => {}
                                InstigatingSource::ProjectNode(
                                    path,
                                    _,
                                    project_node,
                                    _,
                                    mut project,
                                ) => {
                                    if key == "Attributes" {
                                        match changed_value {
                                            Variant::Attributes(attributes) => {
                                                for (name, value) in attributes.to_owned() {
                                                    let unresolved_value =
                                                        UnresolvedValue::FullyQualified(value);
                                                    match project_node.attributes.get(&name) {
                                                        Some(value2) => {
                                                            if unresolved_value
                                                                .to_owned()
                                                                .resolve_unambiguous()
                                                                .unwrap()
                                                                != value2
                                                                    .to_owned()
                                                                    .resolve_unambiguous()
                                                                    .unwrap()
                                                            {
                                                                let tree = change_attributes_in_project(
                                                                    &mut project_node.to_owned(),
                                                                    &mut project.tree,
                                                                    key.to_owned(),
                                                                    unresolved_value.to_owned(),
                                                                );
                                                                let tree: &_ = tree;
                                                                project.tree = tree.clone();
                                                                used_paths.push(path.clone());
                                                                change_bypass.insert(path.to_path_buf());
                                                                fs::write(
                                                                    path.to_owned(),
                                                                    serde_json::to_string_pretty(&project).unwrap(),
                                                                )
                                                                .unwrap();
                                                                continue;
                                                            }
                                                        }
                                                        None => {}
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    } else {
                                        let unresolved_value = UnresolvedValue::FullyQualified(
                                            changed_value.to_owned(),
                                        );
                                        if let Some(value2) = project_node.properties.get(key) {
                                            if unresolved_value
                                                .to_owned()
                                                .resolve_unambiguous()
                                                .unwrap()
                                                != value2.to_owned().resolve_unambiguous().unwrap()
                                            {
                                                let tree = change_property_in_project(
                                                    &mut project_node.to_owned(),
                                                    &mut project.tree,
                                                    key.to_owned(),
                                                    unresolved_value.to_owned(),
                                                );
                                                let tree: &_ = tree;
                                                project.tree = tree.clone();
                                                used_paths.push(path.clone());
                                                change_bypass.insert(path.to_path_buf());
                                                fs::write(
                                                    path.to_owned(),
                                                    serde_json::to_string_pretty(&project).unwrap(),
                                                )
                                                .unwrap();
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        for path in &instance.metadata().relevant_paths {
                            if path.file_name_ends_with(".rbxm")
                                || path.file_name_ends_with(".rbxmx")
                            {
                                if !rbxm_files_to_update.contains(&(instance.id(), path.clone())) {
                                    used_paths.push(path.clone());
                                    change_bypass.insert(path.to_path_buf());
                                    rbxm_files_to_update.push((instance.id(), path.clone()));
                                }
                                continue 'changed_properties;
                            } else if path.file_name_ends_with(".lua") && key == "Source" {
                                if let Some(Variant::String(value)) = changed_value {
                                    used_paths.push(path.clone());
                                    change_bypass.insert(path.to_path_buf());

                                    fs::write(path, value).unwrap();
                                } else {
                                    log::warn!("Cannot change Source to non-string value.");
                                }
                            } else if path.file_name_ends_with(".meta.json") {
                                if path.ends_with("init.meta.json") {
                                    let mut metadata =
                                        match self.vfs.read(&path).with_not_found().unwrap() {
                                            Some(meta_contents) => DirectoryMetadata::from_slice(
                                                &meta_contents,
                                                path.clone(),
                                            )
                                            .unwrap(),
                                            None => DirectoryMetadata::new(path.clone()),
                                        };
                                    metadata.insert(
                                        key.clone(),
                                        changed_value.as_ref().unwrap().clone(),
                                    );
                                    let data = serde_json::to_string_pretty(&metadata).unwrap();
                                    used_paths.push(path.clone());
                                    change_bypass.insert(path.clone());
                                    fs::write(path, data).unwrap();
                                } else {
                                    let mut metadata =
                                        match self.vfs.read(&path).with_not_found().unwrap() {
                                            Some(meta_contents) => AdjacentMetadata::from_slice(
                                                &meta_contents,
                                                path.clone(),
                                            )
                                            .unwrap(),
                                            None => AdjacentMetadata::new(path.clone()),
                                        };
                                    metadata.insert(
                                        key.clone(),
                                        changed_value.as_ref().unwrap().clone(),
                                    );
                                    let data = serde_json::to_string_pretty(&metadata).unwrap();
                                    used_paths.push(path.clone());
                                    change_bypass.insert(path.clone());
                                    fs::write(path, data).unwrap();
                                };
                            }
                        }
                    }
                } else {
                    log::warn!("Cannot update instance {:?}, it does not exist.", id);
                }
            }

            let apply_patch_set = apply_patch_set(&mut tree, patch_set);

            for (id, path) in rbxm_files_to_update {
                match write_model(&tree, &path, id) {
                    Ok(_) => { }
                    Err(error) => {
                        log::warn!(
                            "Cannot update rbxm or rbxml at path {:?}, because of error: {:?}.",
                            path,
                            error
                        );
                    }
                };
            }
            apply_patch_set
        };

        if !applied_patch.is_empty() {
            self.message_queue.push_messages(&[applied_patch]);
        }

        let  mut applied_patches =  Vec::new();
        for path in used_paths {
            let affected_ids =  tree.get_ids_at_path(&path.as_path()).to_vec();
            for id in affected_ids {
                if let Some(patch) = compute_and_apply_changes(&mut tree, &self.vfs, &mut generation_map, id) {
                    if !patch.is_empty() {
                        applied_patches.push(patch);
                    }
                }
            }
        }
        if !applied_patches.is_empty() {
            self.message_queue.push_messages(&applied_patches);
        }
    }
}
fn on_vfs_event(
    path: PathBuf,
    tree: &Arc<Mutex<RojoTree>>,
    vfs: &Arc<Vfs>,
    generation_map: &Arc<Mutex<GenerationMap>>,
    change_bypass: &Arc<Mutex<ChangeBypass>>,
) -> Vec<AppliedPatchSet> {
    let mut change_bypass = change_bypass.lock().unwrap();
    if change_bypass.needs_bypass(path.clone()) {
        return Vec::new();
    }
    let mut tree = tree.lock().unwrap();
    let mut generation_map = generation_map.lock().unwrap();
    let mut applied_patches = Vec::new();

    let mut current_path = path.as_path();

    // Find the nearest ancestor to this path that has
    // associated instances in the tree. This helps make sure
    // that we handle additions correctly, especially if we
    // receive events for descendants of a large tree being
    // created all at once.
    let affected_ids = loop {
        let ids = tree.get_ids_at_path(&current_path);

        log::trace!("Path {} affects IDs {:?}", current_path.display(), ids);

        if !ids.is_empty() {
            break ids.to_vec();
        }

        log::trace!("Trying parent path...");
        match current_path.parent() {
            Some(parent) => current_path = parent,
            None => break Vec::new(),
        }
    };

    for id in affected_ids {
        if let Some(patch) = compute_and_apply_changes(&mut tree, &vfs, &mut generation_map, id) {
            if !patch.is_empty() {
                applied_patches.push(patch);
            }
        }
    }
    applied_patches
}
fn compute_and_apply_changes(
    tree: &mut RojoTree,
    vfs: &Vfs,
    generation_map: &mut GenerationMap,
    id: Ref,
) -> Option<AppliedPatchSet> {
    let metadata = tree
        .get_metadata(id)
        .expect("metadata missing for instance present in tree");

    let instigating_source = match &metadata.instigating_source {
        Some(path) => path,
        None => {
            log::error!(
                "Instance {:?} did not have an instigating source, but was considered for an update.",
                id
            );
            log::error!("This is a bug. Please file an issue!");
            return None;
        }
    };
    // How we process a file change event depends on what created this
    // file/folder in the first place.
    let applied_patch_set = match instigating_source {
        InstigatingSource::Path(path) => match vfs.metadata(path).with_not_found() {
            Ok(Some(_)) => {
                if path.is_dir() {
                    generation_map.next_generation(path.clone());
                } else {
                    generation_map.next_generation(path.parent().unwrap().to_path_buf());
                }
                // Our instance was previously created from a path and that
                // path still exists. We can generate a snapshot starting at
                // that path and use it as the source for our patch.
                let snapshot =
                    match snapshot_from_vfs(&metadata.context, &vfs, &path, generation_map) {
                        Ok(snapshot) => snapshot,
                        Err(err) => {
                            log::error!("Snapshot error: {:?}", err);
                            return None;
                        }
                    };

                let patch_set = compute_patch_set(snapshot, &tree, id);
                apply_patch_set(tree, patch_set)
            }
            Ok(None) => {
                // Our instance was previously created from a path, but that
                // path no longer exists.
                //
                // We associate deleting the instigating file for an
                // instance with deleting that instance.

                let mut patch_set = PatchSet::new();
                patch_set.removed_instances.push(id);

                apply_patch_set(tree, patch_set)
            }
            Err(err) => {
                log::error!("Error processing filesystem change: {:?}", err);
                return None;
            }
        },

        InstigatingSource::ProjectNode(
            project_path,
            instance_name,
            project_node,
            parent_class,
            project,
        ) => {
            // This instance is the direct subject of a project node. Since
            // there might be information associated with our instance from
            // the project file, we snapshot the entire project node again.

            let snapshot_result = snapshot_project_node(
                &metadata.context,
                &project_path,
                instance_name,
                project_node,
                &vfs,
                parent_class.as_ref().map(|name| name.as_str()),
                generation_map,
                project,
            );

            let snapshot = match snapshot_result {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    log::error!("{:?}", err);
                    return None;
                }
            };

            let patch_set = compute_patch_set(snapshot, &tree, id);
            apply_patch_set(tree, patch_set)
        }
    };

    Some(applied_patch_set)
}

fn write_model(tree: &MutexGuard<RojoTree>, path: &PathBuf, root_id: Ref) -> anyhow::Result<()> {
    log::trace!("Opening output file for write");
    let mut file = BufWriter::new(File::create(path)?);
    let extension = path.extension().unwrap();
    if extension == "rbxm" {
        rbx_binary::to_writer(&mut file, tree.inner(), &[root_id])?;
    } else if extension == "rbxmx" {
        rbx_xml::to_writer(&mut file, tree.inner(), &[root_id], xml_encode_config())?;
    }

    file.flush()?;

    Ok(())
}

fn xml_encode_config() -> rbx_xml::EncodeOptions {
    rbx_xml::EncodeOptions::new().property_behavior(rbx_xml::EncodePropertyBehavior::WriteUnknown)
}

fn change_property_in_project<'a>(
    node: &mut ProjectNode,
    tree: &'a mut ProjectNode,
    key: String,
    value: UnresolvedValue,
) -> &'a mut ProjectNode {
    for (_, child) in tree.children.iter_mut() {
        if child == node {
            child.properties.insert(key.to_owned(), value.to_owned());
        }
        change_property_in_project(node, child, key.to_owned(), value.to_owned());
    }
    return tree;
}

fn change_attributes_in_project<'a>(
    node: &mut ProjectNode,
    tree: &'a mut ProjectNode,
    key: String,
    value: UnresolvedValue,
) -> &'a mut ProjectNode {
    for (_, child) in tree.children.iter_mut() {
        if child == node {
            child.properties.insert(key.to_owned(), value.to_owned());
        }
        change_property_in_project(node, child, key.to_owned(), value.to_owned());
    }
    return tree;
}
