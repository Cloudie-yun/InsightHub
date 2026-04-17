// ============================================================================
// CHAT PAGE SHARED STATE, ELEMENT REFERENCES, AND HELPERS
// ============================================================================
// Every chat sub-module attaches to the same `window.ChatPage` namespace so the
// page can stay on plain browser scripts without changing runtime behavior.
(function initializeChatSharedNamespace() {
    const ns = window.ChatPage = window.ChatPage || {};

    ns.elements = {
        workspace: document.getElementById("chat-workspace"),
        panel: document.getElementById("floating-tools-panel"),
        chatMain: document.getElementById("chat-main"),
        dragHandle: document.getElementById("floating-tools-drag-handle"),
        panelTitle: document.getElementById("floating-tools-title"),
        toolsMenu: document.getElementById("floating-tools-title-container"),
        collapseButton: document.getElementById("floating-tools-collapse"),
        resizeHandle: document.getElementById("floating-tools-resize-handle"),
        sourcesSection: document.getElementById("sources-section"),
        studyAidSection: document.getElementById("study-aid-section"),
        sourcesHeader: document.getElementById("sources-header"),
        studyAidHeader: document.getElementById("study-aid-header"),
        sourcesAddBtn: document.getElementById("sources-add-btn"),
        sourcesControls: document.getElementById("sources-controls"),
        sourcesSearchInput: document.getElementById("sources-search-input"),
        sourcesSortSelect: document.getElementById("sources-sort-select"),
        sourcesSelectAllBtn: document.getElementById("sources-select-all-btn"),
        sourcesEmptyState: document.getElementById("sources-empty-state"),
        sourcesBody: document.getElementById("sources-body"),
        studyAidBody: document.getElementById("study-aid-body"),
        sourcesDetailedList: document.getElementById("sources-detailed-list"),
        studyAidDetailedList: document.getElementById("study-aid-detailed-list"),
        sourcesIconList: document.getElementById("sources-icon-list"),
        sourcesCollapsedSummary: document.getElementById("sources-collapsed-summary"),
        sourcesCollapsedIcon: document.getElementById("sources-collapsed-icon"),
        sourcesCollapsedCount: document.getElementById("sources-collapsed-count"),
        studyAidIconList: document.getElementById("study-aid-icon-list"),
        toolsDefaultContent: document.getElementById("floating-tools-default-content"),
        toolsDocumentContent: document.getElementById("floating-tools-document-content"),
        sendButton: document.getElementById("chat-send-button"),
        promptInput: document.getElementById("chat-prompt-input"),
        sendStatus: document.getElementById("chat-send-status"),
        toolboxDocFrame: document.getElementById("toolbox-doc-frame"),
        toolboxDocEmpty: document.getElementById("toolbox-doc-empty"),
        toolboxDocTitle: document.getElementById("toolbox-doc-title"),
        toolboxDocOpen: document.getElementById("toolbox-doc-open"),
        toolboxDocBack: document.getElementById("toolbox-doc-back"),
        chatMessages: document.getElementById("chat-messages"),
        promptRail: document.getElementById("prompt-rail"),
        promptRailNodes: document.getElementById("prompt-rail-nodes"),
        conversationPanel: document.getElementById("chat-conversation-panel"),
        chatUploadModal: document.getElementById("chat-upload-modal"),
        chatUploadModalBackdrop: document.getElementById("chat-upload-modal-backdrop"),
        chatUploadDropzone: document.getElementById("chat-upload-dropzone"),
        chatUploadInput: document.getElementById("chat-upload-input"),
        chatUploadBrowseBtn: document.getElementById("chat-upload-browse-btn"),
        chatUploadCancelBtn: document.getElementById("chat-upload-cancel-btn"),
        chatUploadConfirmBtn: document.getElementById("chat-upload-confirm-btn"),
        chatUploadClearBtn: document.getElementById("chat-upload-clear-btn"),
        chatUploadStatus: document.getElementById("chat-upload-status"),
        chatUploadSelectionSummary: document.getElementById("chat-upload-selection-summary"),
        chatUploadCount: document.getElementById("chat-upload-count"),
        chatUploadFileList: document.getElementById("chat-upload-file-list"),
        chatDropOverlay: document.getElementById("chat-drop-overlay"),
    };

    ns.state = {
        dragging: false,
        resizing: false,
        dragOffsetX: 0,
        resizeStartX: 0,
        resizeStartWidth: 340,
        dockSide: "left",
        isPanelCollapsed: false,
        expandedPanelWidth: 340,
        isToolboxDocumentMode: false,
        widthBeforeDocumentMode: null,
        promptScrollTicking: false,
        promptAnchors: [],
        isConversationUploading: false,
        conversationDragDepth: 0,
        pendingConversationUploadFiles: [],
        pendingSourceStatusPollHandle: null,
        promptNodeButtons: new Map(),
        sourceSearchQuery: "",
        sourceSortMode: "upload_desc",
        isSendingMessage: false,
        nextPromptIndex: 0,
        promptRailBound: false,
    };

    ns.constants = {
        PANEL_STORAGE_KEY: "chat.toolsPanelState",
        PENDING_PARSER_STATUS: "pending",
        PENDING_SOURCE_POLL_INTERVAL_MS: 5000,
        PANEL_GUTTER: 8,
        PANEL_COLLAPSED_WIDTH: 80,
        PANEL_MIN_WIDTH: 260,
        PANEL_MAX_WIDTH: 620,
        DEFAULT_SOURCE_SORT_MODE: "upload_desc",
        ALLOWED_CONVERSATION_UPLOAD_EXTENSIONS: new Set([
            ".pdf",
            ".doc",
            ".docx",
            ".ppt",
            ".pptx",
            ".png",
            ".jpg",
            ".jpeg",
            ".webp",
        ]),
    };

    ns.clamp = (value, min, max) => Math.min(Math.max(value, min), max);
    ns.getCurrentPanelWidth = () => (
        ns.state.isPanelCollapsed
            ? ns.constants.PANEL_COLLAPSED_WIDTH
            : ns.state.expandedPanelWidth
    );
    ns.getUploadUrl = (fileName) => `/uploads/${encodeURIComponent(fileName)}`;
    ns.getPreviewUrl = (fileName) => `/uploads/preview/${encodeURIComponent(fileName)}`;
    ns.getMaxAllowedPanelWidth = () => (
        Math.min(ns.constants.PANEL_MAX_WIDTH, ns.elements.workspace.clientWidth - 120)
    );
    ns.getCurrentConversationId = () => String(window.__CURRENT_CONVERSATION_ID__ || "").trim();

    ns.notify = (type, message) => {
        if (typeof window.notify === "function") {
            window.notify({ type, message });
            return;
        }

        const toastMethod = window.toast && typeof window.toast[type] === "function"
            ? window.toast[type]
            : null;

        if (toastMethod) {
            toastMethod(message);
            return;
        }

        if (message) {
            window.alert(message);
        }
    };

    ns.hasDraggedFiles = (event) => {
        const types = Array.from(event.dataTransfer?.types || []);
        return types.includes("Files");
    };

    ns.setChatUploadStatus = (message, isError = false) => {
        const { chatUploadStatus } = ns.elements;
        if (!chatUploadStatus) return;
        chatUploadStatus.textContent = message || "";
        chatUploadStatus.className = `text-sm text-center min-h-[20px] ${isError ? "text-red-600" : "text-slate-500"}`;
    };

    ns.setConversationDropOverlay = (active) => {
        const { chatDropOverlay } = ns.elements;
        if (!chatDropOverlay) return;
        chatDropOverlay.classList.toggle("hidden", !active);
    };

    ns.getFileExtension = (fileName) => {
        const safeName = String(fileName || "").trim().toLowerCase();
        const dotIndex = safeName.lastIndexOf(".");
        if (dotIndex <= 0) return "";
        return safeName.slice(dotIndex);
    };

    ns.formatFileCountLabel = (count) => `${count} file${count === 1 ? "" : "s"}`;

    ns.formatDurationLabel = (seconds) => {
        const safeSeconds = Math.max(1, Math.round(seconds));
        if (safeSeconds < 60) return `${safeSeconds}s`;
        const mins = Math.round(safeSeconds / 60);
        return `${mins} min${mins === 1 ? "" : "s"}`;
    };

    ns.estimateProcessingSecondsForFile = (file) => {
        const sizeMb = Math.max(0, Number(file?.size || 0) / (1024 * 1024));
        // Rough heuristic for upload + document parse.
        // Tuned to avoid under-promising on medium/large PDFs.
        const seconds = 18 + (sizeMb * 4.2);
        return Math.min(900, Math.max(20, Math.round(seconds)));
    };

    ns.estimateProcessingWindowForFiles = (files) => {
        const estimates = files.map(ns.estimateProcessingSecondsForFile);
        const total = estimates.reduce((sum, value) => sum + value, 0);
        const minSeconds = Math.round(total * 0.75);
        const maxSeconds = Math.round(total * 1.35);
        return {
            minSeconds,
            maxSeconds,
            label: `${ns.formatDurationLabel(minSeconds)} - ${ns.formatDurationLabel(maxSeconds)}`,
        };
    };

    ns.getParserProgressMessage = (documentPayload) => {
        const parserProgress = documentPayload?.parser_progress;
        if (parserProgress && typeof parserProgress.message === "string" && parserProgress.message.trim()) {
            return parserProgress.message.trim();
        }
        const providerState = String(parserProgress?.provider_state || "").trim().toLowerCase();
        if (providerState) {
            return `Processing document (${providerState})...`;
        }
        return "Processing document...";
    };

    ns.splitValidAndInvalidUploadFiles = (fileList) => {
        const files = Array.from(fileList || []);
        const validFiles = [];
        const invalidFileNames = [];

        files.forEach((file) => {
            const extension = ns.getFileExtension(file?.name);
            if (!ns.constants.ALLOWED_CONVERSATION_UPLOAD_EXTENSIONS.has(extension)) {
                invalidFileNames.push(file?.name || "Unnamed file");
                return;
            }
            validFiles.push(file);
        });

        return { validFiles, invalidFileNames };
    };

    ns.buildUnsupportedFileWarning = (invalidFileNames) => {
        const invalidPreview = invalidFileNames.slice(0, 2).join(", ");
        const remainder = invalidFileNames.length > 2 ? ` and ${invalidFileNames.length - 2} more` : "";
        return `Some files were skipped due to unsupported format: ${invalidPreview}${remainder}.`;
    };

    ns.notifyUnsupportedUploadFiles = (invalidFileNames) => {
        if (!Array.isArray(invalidFileNames) || invalidFileNames.length === 0) return;
        ns.notify("warning", ns.buildUnsupportedFileWarning(invalidFileNames));
    };

    ns.escapeHtml = (value) => String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");

    ns.setChatSendStatus = (message, isError = false) => {
        const { sendStatus } = ns.elements;
        if (!sendStatus) return;
        sendStatus.textContent = message || "";
        sendStatus.className = `mt-2 min-h-[20px] text-center text-[13px] ${isError ? "text-red-600" : "text-slate-500"}`;
    };
}());
