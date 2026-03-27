// ============================================================================
// CHAT PAGE FILE UPLOAD FLOWS
// ============================================================================
// This module contains modal state, file validation, drag-and-drop upload
// flows, and the UI refresh logic that runs after each upload request.
(function initializeChatUploadModule() {
    const ns = window.ChatPage;
    if (!ns) return;

    const {
        chatMain,
        conversationPanel,
        sourcesAddBtn,
        chatUploadModal,
        chatUploadModalBackdrop,
        chatUploadDropzone,
        chatUploadInput,
        chatUploadBrowseBtn,
        chatUploadCancelBtn,
        chatUploadConfirmBtn,
        chatUploadClearBtn,
        chatUploadSelectionSummary,
        chatUploadCount,
        chatUploadFileList,
    } = ns.elements;
    const state = ns.state;
    const constants = ns.constants;

    ns.renderPendingUploadFiles = () => {
        if (!chatUploadSelectionSummary || !chatUploadFileList || !chatUploadCount) return;

        const hasFiles = state.pendingConversationUploadFiles.length > 0;
        chatUploadSelectionSummary.classList.toggle("hidden", !hasFiles);
        chatUploadCount.textContent = ns.formatFileCountLabel(state.pendingConversationUploadFiles.length);
        chatUploadFileList.replaceChildren();

        state.pendingConversationUploadFiles.forEach((file) => {
            const item = document.createElement("li");
            item.className = "truncate";
            item.title = file.name;
            item.textContent = file.name;
            chatUploadFileList.appendChild(item);
        });

        if (chatUploadConfirmBtn) {
            chatUploadConfirmBtn.disabled = !hasFiles || state.isConversationUploading;
        }
    };

    ns.resetPendingUploadFiles = () => {
        state.pendingConversationUploadFiles = [];
        ns.renderPendingUploadFiles();
    };

    ns.queueConversationUploadFiles = (fileList, options = {}) => {
        const { validFiles, invalidFileNames } = ns.splitValidAndInvalidUploadFiles(fileList);
        if (!validFiles.length && !invalidFileNames.length) return;

        ns.notifyUnsupportedUploadFiles(invalidFileNames);

        state.pendingConversationUploadFiles = validFiles;
        if (chatUploadInput) {
            chatUploadInput.value = "";
        }

        if (!validFiles.length) {
            ns.setChatUploadStatus("No supported files selected.", true);
        } else {
            ns.setChatUploadStatus(
                `Ready to upload ${ns.formatFileCountLabel(validFiles.length)}. Please confirm.`,
                false,
            );
        }

        ns.renderPendingUploadFiles();
        if (options.openModal !== false) {
            ns.openChatUploadModal();
        }
    };

    ns.uploadDroppedFilesImmediately = (fileList) => {
        const { validFiles, invalidFileNames } = ns.splitValidAndInvalidUploadFiles(fileList);
        if (!validFiles.length && !invalidFileNames.length) return;

        ns.notifyUnsupportedUploadFiles(invalidFileNames);

        if (!validFiles.length) {
            ns.notify("warning", "No supported files were dropped.");
            return;
        }

        ns.uploadFilesToCurrentConversation(validFiles, { closeModalOnSuccess: false });
    };

    ns.openChatUploadModal = () => {
        if (!chatUploadModal) return;
        chatUploadModal.classList.remove("hidden");
        chatUploadModal.classList.add("flex");
        ns.renderPendingUploadFiles();
    };

    ns.closeChatUploadModal = (options = {}) => {
        const { force = false } = options;
        if (state.isConversationUploading && !force) return;
        if (!chatUploadModal) return;

        chatUploadModal.classList.add("hidden");
        chatUploadModal.classList.remove("flex");
        ns.setChatUploadStatus("");
        ns.resetPendingUploadFiles();
        if (chatUploadInput) {
            chatUploadInput.value = "";
        }
    };

    ns.uploadFilesToCurrentConversation = async (fileList, options = {}) => {
        const files = Array.from(fileList || []);
        if (!files.length || state.isConversationUploading) return;

        const conversationId = ns.getCurrentConversationId();
        if (!window.__AUTH_USER__ || !window.__AUTH_USER__.user_id) {
            ns.notify("warning", "Please log in to upload documents.");
            ns.setChatUploadStatus("Please log in to upload documents.", true);
            return;
        }
        if (!conversationId) {
            ns.notify("warning", "Please start or open a conversation first.");
            ns.setChatUploadStatus("No conversation selected.", true);
            return;
        }

        state.isConversationUploading = true;
        if (chatUploadConfirmBtn) {
            chatUploadConfirmBtn.disabled = true;
        }

        const estimateWindow = ns.estimateProcessingWindowForFiles(files);
        ns.setChatUploadStatus(
            `Uploading ${ns.formatFileCountLabel(files.length)}... Estimated processing time: ${estimateWindow.label}.`,
        );

        const processingItemIds = ns.createProcessingSourceItems(files);
        const formData = new FormData();
        files.forEach((file) => formData.append("documents", file));

        let payload = {};
        try {
            const response = await fetch(`/api/conversations/${encodeURIComponent(conversationId)}/documents/upload`, {
                method: "POST",
                body: formData,
            });
            try {
                payload = await response.json();
            } catch (_error) {
                payload = {};
            }

            if (!response.ok) {
                const errorMessage = payload.error || "Upload failed. Please try again.";
                ns.removeProcessingSourceItems(processingItemIds);
                ns.setChatUploadStatus(errorMessage, true);
                ns.notify("error", errorMessage);
                return;
            }
        } catch (error) {
            console.error("Conversation upload request failed:", error);
            const networkError = "Network error while uploading documents.";
            ns.removeProcessingSourceItems(processingItemIds);
            ns.setChatUploadStatus(networkError, true);
            ns.notify("error", networkError);
            return;
        }

        try {
            const uploadedDocuments = Array.isArray(payload.documents) ? payload.documents : [];
            ns.removeProcessingSourceItems(processingItemIds);

            try {
                await ns.refreshToolboxSourcesSection();
            } catch (refreshError) {
                console.error("Toolbox refresh failed, using upload payload fallback:", refreshError);
                uploadedDocuments.forEach((doc) => {
                    // Mark fallback cards as dirty so the first poll cannot be skipped
                    // by a stale upload-time signature.
                    ns.upsertSourceItems(doc, { forceDirty: true });
                });
                ns.updateSelectAllButtonState();
                ns.updateSendButtonState();
            }

            const successMessage = payload.message || `Uploaded ${uploadedDocuments.length} file(s).`;
            ns.setChatUploadStatus(successMessage, false);
            ns.notify("success", successMessage);

            if (options.closeModalOnSuccess !== false) {
                ns.closeChatUploadModal({ force: true });
            }
        } catch (error) {
            console.error("Conversation upload UI refresh failed:", error);
            const uiWarn = "Uploaded successfully. Sources panel sync is delayed; it will auto-update shortly.";
            ns.setChatUploadStatus(uiWarn, false);
            ns.notify("warning", uiWarn);
        } finally {
            state.isConversationUploading = false;
            ns.renderPendingUploadFiles();
            // Always resume polling after an upload if any pending cards exist,
            // even when the immediate refresh path had to fall back.
            if (ns.getPendingDocumentIds().size) {
                ns.ensurePendingSourcePolling();
                ns.refreshPendingSourceStates();
            }
        }
    };

    ns.bindUploadDropzoneInteractions = () => {
        chatUploadDropzone?.addEventListener("dragover", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            chatUploadDropzone.classList.add("border-brand-500", "bg-brand-100/40");
        });
        chatUploadDropzone?.addEventListener("dragleave", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            chatUploadDropzone.classList.remove("border-brand-500", "bg-brand-100/40");
        });
        chatUploadDropzone?.addEventListener("drop", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            chatUploadDropzone.classList.remove("border-brand-500", "bg-brand-100/40");
            ns.queueConversationUploadFiles(event.dataTransfer.files, { openModal: true });
        });
    };

    ns.bindConversationDropOverlayInteractions = () => {
        const dragArea = conversationPanel || chatMain;
        dragArea?.addEventListener("dragenter", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            state.conversationDragDepth += 1;
            ns.setConversationDropOverlay(true);
        });
        dragArea?.addEventListener("dragover", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            event.dataTransfer.dropEffect = "copy";
        });
        dragArea?.addEventListener("dragleave", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            state.conversationDragDepth = Math.max(0, state.conversationDragDepth - 1);
            if (state.conversationDragDepth === 0) {
                ns.setConversationDropOverlay(false);
            }
        });
        dragArea?.addEventListener("drop", (event) => {
            if (!ns.hasDraggedFiles(event)) return;
            event.preventDefault();
            state.conversationDragDepth = 0;
            ns.setConversationDropOverlay(false);
            ns.uploadDroppedFilesImmediately(event.dataTransfer.files);
        });

        window.addEventListener("dragend", () => {
            state.conversationDragDepth = 0;
            ns.setConversationDropOverlay(false);
        });
        window.addEventListener("drop", () => {
            state.conversationDragDepth = 0;
            ns.setConversationDropOverlay(false);
        });
    };

    ns.initializeConversationUpload = () => {
        sourcesAddBtn?.addEventListener("click", ns.openChatUploadModal);
        chatUploadCancelBtn?.addEventListener("click", ns.closeChatUploadModal);
        chatUploadModalBackdrop?.addEventListener("click", ns.closeChatUploadModal);
        chatUploadBrowseBtn?.addEventListener("click", () => chatUploadInput?.click());
        chatUploadClearBtn?.addEventListener("click", () => {
            ns.resetPendingUploadFiles();
            ns.setChatUploadStatus("");
        });
        chatUploadConfirmBtn?.addEventListener("click", () => {
            ns.uploadFilesToCurrentConversation(state.pendingConversationUploadFiles);
        });
        chatUploadDropzone?.addEventListener("click", () => {
            if (state.isConversationUploading) return;
            chatUploadInput?.click();
        });
        chatUploadInput?.addEventListener("change", () => {
            ns.queueConversationUploadFiles(chatUploadInput.files, { openModal: true });
        });

        ns.bindUploadDropzoneInteractions();
        ns.bindConversationDropOverlayInteractions();
    };
}());
