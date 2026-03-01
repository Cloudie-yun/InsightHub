const workspace = document.getElementById("chat-workspace");
const panel = document.getElementById("floating-tools-panel");
const chatMain = document.getElementById("chat-main");
const dragHandle = document.getElementById("floating-tools-drag-handle");
const panelTitle = document.getElementById("floating-tools-title");
const toolsMenu = document.getElementById("floating-tools-title-container");
const collapseButton = document.getElementById("floating-tools-collapse");
const resizeHandle = document.getElementById("floating-tools-resize-handle");
const sourcesSection = document.getElementById("sources-section");
const studyAidSection = document.getElementById("study-aid-section");
const sourcesHeader = document.getElementById("sources-header");
const studyAidHeader = document.getElementById("study-aid-header");
const sourcesAddBtn = document.getElementById("sources-add-btn");
const sourcesSelectAllBtn = document.getElementById("sources-select-all-btn");
const sourcesBody = document.getElementById("sources-body");
const studyAidBody = document.getElementById("study-aid-body");
const sourcesDetailedList = document.getElementById("sources-detailed-list");
const studyAidDetailedList = document.getElementById("study-aid-detailed-list");
const sourcesIconList = document.getElementById("sources-icon-list");
const studyAidIconList = document.getElementById("study-aid-icon-list");
const toolsDefaultContent = document.getElementById("floating-tools-default-content");
const toolsDocumentContent = document.getElementById("floating-tools-document-content");
const sendButton = document.querySelector("#chat-main .fa-arrow-up")?.closest("button");
const toolboxDocFrame = document.getElementById("toolbox-doc-frame");
const toolboxDocEmpty = document.getElementById("toolbox-doc-empty");
const toolboxDocTitle = document.getElementById("toolbox-doc-title");
const toolboxDocOpen = document.getElementById("toolbox-doc-open");
const toolboxDocBack = document.getElementById("toolbox-doc-back");
const chatMessages = document.getElementById("chat-messages");
const promptRail = document.getElementById("prompt-rail");
const promptRailNodes = document.getElementById("prompt-rail-nodes");

let dragging = false;
let resizing = false;
let dragOffsetX = 0;
let resizeStartX = 0;
let resizeStartWidth = 320;
let dockSide = "left";
let isPanelCollapsed = false;
let expandedPanelWidth = 320;
let isToolboxDocumentMode = false;
let widthBeforeDocumentMode = null;
let promptScrollTicking = false;
let promptAnchors = [];
const promptNodeButtons = new Map();
const PANEL_STORAGE_KEY = "chat.toolsPanelState";

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);
const PANEL_GUTTER = 8;
const PANEL_COLLAPSED_WIDTH = 80;
const PANEL_MIN_WIDTH = 260;
const PANEL_MAX_WIDTH = 620;
const getCurrentPanelWidth = () => (isPanelCollapsed ? PANEL_COLLAPSED_WIDTH : expandedPanelWidth);
const getUploadUrl = (fileName) => `/uploads/${encodeURIComponent(fileName)}`;
const getPreviewUrl = (fileName) => `/uploads/preview/${encodeURIComponent(fileName)}`;
const getMaxAllowedPanelWidth = () => Math.min(PANEL_MAX_WIDTH, workspace.clientWidth - 120);

const savePanelState = () => {
    const payload = {
        collapsed: isPanelCollapsed,
        width: expandedPanelWidth,
        side: dockSide,
    };
    localStorage.setItem(PANEL_STORAGE_KEY, JSON.stringify(payload));
};

const loadPanelState = () => {
    try {
        const raw = localStorage.getItem(PANEL_STORAGE_KEY);
        if (!raw) return;
        const parsed = JSON.parse(raw);
        if (typeof parsed.collapsed === "boolean") {
            isPanelCollapsed = parsed.collapsed;
        }
        if (typeof parsed.width === "number") {
            expandedPanelWidth = clamp(parsed.width, PANEL_MIN_WIDTH, PANEL_MAX_WIDTH);
        }
        if (parsed.side === "left" || parsed.side === "right") {
            dockSide = parsed.side;
        }
    } catch (_error) {
        // Ignore invalid persisted state and fallback to defaults.
    }
};

const updateSectionLayout = () => {
    if (isPanelCollapsed) {
        sourcesBody.classList.remove("hidden");
        studyAidBody.classList.remove("hidden");
        sourcesSection.classList.remove("flex-1");
        studyAidSection.classList.remove("flex-1");
        sourcesSection.classList.add("flex-none");
        studyAidSection.classList.add("flex-none");
        return;
    }

    const sourcesOpen = sourcesSection.dataset.open === "true";
    const studyAidOpen = studyAidSection.dataset.open === "true";
    sourcesBody.classList.toggle("hidden", !sourcesOpen);
    studyAidBody.classList.toggle("hidden", !studyAidOpen);
    sourcesSection.classList.toggle("flex-1", sourcesOpen);
    sourcesSection.classList.toggle("flex-none", !sourcesOpen);
    studyAidSection.classList.toggle("flex-1", studyAidOpen);
    studyAidSection.classList.toggle("flex-none", !studyAidOpen);
};

const applyPanelCollapseState = () => {
    // Hide the entire drag handle + collapse button when in document mode
    dragHandle.classList.toggle("hidden", isToolboxDocumentMode);
    
    if (isPanelCollapsed && isToolboxDocumentMode) {
        closeToolboxDocument();
    }

    if (!isToolboxDocumentMode) {
        panelTitle.classList.toggle("hidden", isPanelCollapsed);
        toolsMenu.classList.toggle("hidden", isPanelCollapsed);
        dragHandle.classList.toggle("justify-center", isPanelCollapsed);
        dragHandle.classList.toggle("justify-between", !isPanelCollapsed);
    }

    sourcesHeader.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    studyAidHeader.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    sourcesAddBtn.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    sourcesSelectAllBtn.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    sourcesDetailedList.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    studyAidDetailedList.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    sourcesIconList.classList.toggle("hidden", !isPanelCollapsed || isToolboxDocumentMode);
    studyAidIconList.classList.toggle("hidden", !isPanelCollapsed || isToolboxDocumentMode);

    if (toolsDefaultContent) {
        toolsDefaultContent.style.display = isToolboxDocumentMode ? "none" : "flex";
    }
    if (toolsDocumentContent) {
        const showDoc = isToolboxDocumentMode && !isPanelCollapsed;
        toolsDocumentContent.style.display = showDoc ? "flex" : "none";
    }

    resizeHandle.classList.toggle("hidden", isPanelCollapsed || isToolboxDocumentMode);
    panel.style.width = isPanelCollapsed ? `${PANEL_COLLAPSED_WIDTH}px` : `${expandedPanelWidth}px`;
    updateSectionLayout();
};

const setSectionState = (sectionName, isOpen) => {
    if (isPanelCollapsed) return;
    const section = sectionName === "sources" ? sourcesSection : studyAidSection;
    section.dataset.open = isOpen ? "true" : "false";
    const toggleIcon = document.querySelector(`[data-chevron-icon="${sectionName}"]`);
    toggleIcon.classList.toggle("fa-chevron-up", isOpen);
    toggleIcon.classList.toggle("fa-chevron-down", !isOpen);
    updateSectionLayout();
};

const applySourceSelectButtonState = (button, isSelected) => {
    button.dataset.selected = isSelected ? "true" : "false";
    button.setAttribute("aria-pressed", isSelected ? "true" : "false");
    button.title = isSelected ? "Deselect Document" : "Select Document";

    button.classList.toggle("bg-brand-600", isSelected);
    button.classList.toggle("border-brand-600", isSelected);
    button.classList.toggle("text-white", isSelected);
    button.classList.toggle("hover:bg-brand-700", isSelected);
    button.classList.toggle("hover:text-white", isSelected);

    button.classList.toggle("bg-transparent", !isSelected);
    button.classList.toggle("border-gray-300", !isSelected);
    button.classList.toggle("text-gray-400", !isSelected);
    button.classList.toggle("hover:bg-gray-100", !isSelected);
    button.classList.toggle("hover:text-gray-600", !isSelected);
    button.classList.toggle("hover:border-gray-300", !isSelected);
    button.classList.toggle("hover:border-brand-200", false);
    button.classList.toggle("hover:bg-brand-100", false);
    button.classList.toggle("hover:text-brand-600", false);
};

const getSourceSelectButtons = () => {
    if (!sourcesDetailedList) return [];
    return Array.from(sourcesDetailedList.querySelectorAll('[data-source-select-btn="true"]'));
};

const updateSelectAllButtonState = () => {
    if (!sourcesSelectAllBtn) return;
    const sourceButtons = getSourceSelectButtons();
    const selectedCount = sourceButtons.filter((button) => button.dataset.selected === "true").length;
    const allSelected = sourceButtons.length > 0 && selectedCount === sourceButtons.length;
    sourcesSelectAllBtn.textContent = allSelected ? "Deselect All" : "Select All";
};

const updateSendButtonState = () => {
    if (!sendButton) return;
    const selectedCount = getSourceSelectButtons().filter((button) => button.dataset.selected === "true").length;
    const hasSelected = selectedCount > 0;
    sendButton.disabled = !hasSelected;
    sendButton.classList.toggle("opacity-50", !hasSelected);
    sendButton.classList.toggle("cursor-not-allowed", !hasSelected);
};

const initializeSourceSelectionButtons = () => {
    if (!sourcesDetailedList) return;
    const sourceButtons = getSourceSelectButtons();
    sourceButtons.forEach((button) => {
        const isSelected = button.dataset.selected === "true";
        applySourceSelectButtonState(button, isSelected);
        button.addEventListener("click", (event) => {
            event.preventDefault();
            event.stopPropagation();
            const currentlySelected = button.dataset.selected === "true";
            applySourceSelectButtonState(button, !currentlySelected);
            updateSendButtonState();
            updateSelectAllButtonState();
        });
    });
    sourcesSelectAllBtn?.addEventListener("click", () => {
        const buttons = getSourceSelectButtons();
        const allSelected = buttons.length > 0 && buttons.every((button) => button.dataset.selected === "true");
        const nextSelected = !allSelected;
        buttons.forEach((button) => {
            applySourceSelectButtonState(button, nextSelected);
        });
        updateSendButtonState();
        updateSelectAllButtonState();
    });
    updateSendButtonState();
    updateSelectAllButtonState();
};

const showViewerFallback = (fileName) => {
    if (!toolboxDocEmpty) return;
    toolboxDocEmpty.replaceChildren();
    const text = document.createElement("p");
    text.className = "text-sm text-slate-500";
    text.textContent = `Preview is only available for PDF files. Open "${fileName}" in a new tab.`;
    toolboxDocEmpty.appendChild(text);
};

const openToolboxDocument = (fileName) => {
    if (!toolsDocumentContent || !fileName) return;
    if (isPanelCollapsed) {
        isPanelCollapsed = false;
        applyPanelCollapseState();
        snapPanelToSide(dockSide);
        savePanelState();
    }
    if (widthBeforeDocumentMode === null) {
        widthBeforeDocumentMode = expandedPanelWidth;
    }
    expandedPanelWidth = clamp(getMaxAllowedPanelWidth(), PANEL_MIN_WIDTH, PANEL_MAX_WIDTH);
    const fileUrl = getUploadUrl(fileName);
    const isPdf = fileName.toLowerCase().endsWith(".pdf");
    const isDocx = fileName.toLowerCase().endsWith(".docx");
    const previewUrl = isPdf ? fileUrl : (isDocx ? getPreviewUrl(fileName) : null);
    isToolboxDocumentMode = true;

    applyPanelCollapseState();
    snapPanelToSide(dockSide);

    if (toolboxDocTitle) {
        toolboxDocTitle.textContent = fileName;
    }
    if (toolboxDocOpen) {
        toolboxDocOpen.href = previewUrl || fileUrl;
    }

    if (previewUrl) {
        if (toolboxDocFrame) {
            toolboxDocFrame.src = previewUrl;
            toolboxDocFrame.classList.remove("hidden");
        }
        toolboxDocEmpty?.classList.add("hidden");
    } else {
        if (toolboxDocFrame) {
            toolboxDocFrame.removeAttribute("src");
            toolboxDocFrame.classList.add("hidden");
        }
        toolboxDocEmpty?.classList.remove("hidden");
        showViewerFallback(fileName);
    }
};

const closeToolboxDocument = () => {
    isToolboxDocumentMode = false;
    const fallbackWidth = 320;
    const restoredWidth = widthBeforeDocumentMode ?? fallbackWidth;
    expandedPanelWidth = clamp(restoredWidth, PANEL_MIN_WIDTH, getMaxAllowedPanelWidth());
    widthBeforeDocumentMode = null;
    if (toolboxDocFrame) {
        toolboxDocFrame.removeAttribute("src");
        toolboxDocFrame.classList.add("hidden");
    }
    if (toolboxDocTitle) {
        toolboxDocTitle.textContent = "No document selected";
    }
    if (toolboxDocOpen) {
        toolboxDocOpen.href = "#";
    }
    if (toolboxDocEmpty) {
        toolboxDocEmpty.classList.remove("hidden");
        toolboxDocEmpty.textContent = "Preview is available for PDF files. Open this file in a new tab.";
    }
    applyPanelCollapseState();
    snapPanelToSide(dockSide);
};

const initializeDocumentViewer = () => {
    document.querySelectorAll("[data-doc-file]").forEach((trigger) => {
        trigger.addEventListener("click", (event) => {
            if (event.target.closest('[data-source-select-btn="true"]')) return;
            const fileName = trigger.dataset.docFile;
            if (!fileName) return;
            openToolboxDocument(fileName);
        });
    });
    toolboxDocBack?.addEventListener("click", closeToolboxDocument);
};

const updatePromptRailDockSide = () => {
    if (!promptRail) return;
    promptRail.classList.remove("left-3", "right-3");
    promptRail.classList.add(dockSide === "right" ? "left-3" : "right-3");
};

const setActivePromptNode = (promptId) => {
    promptNodeButtons.forEach((button, id) => {
        const isActive = id === promptId;
        button.classList.toggle("active", isActive);
        if (isActive) {
            button.scrollIntoView({ block: "nearest" });
        }
    });
};

const syncActivePromptNodeFromScroll = () => {
    if (!chatMessages || !promptAnchors.length) return;
    const messagesRect = chatMessages.getBoundingClientRect();
    const railTargetY = messagesRect.top + Math.min(messagesRect.height * 0.35, 220);
    let nearestPromptId = promptAnchors[0]?.dataset.promptId || null;
    let nearestDistance = Number.POSITIVE_INFINITY;

    promptAnchors.forEach((anchor) => {
        const anchorDistance = Math.abs(anchor.getBoundingClientRect().top - railTargetY);
        if (anchorDistance < nearestDistance) {
            nearestDistance = anchorDistance;
            nearestPromptId = anchor.dataset.promptId;
        }
    });

    if (nearestPromptId) {
        setActivePromptNode(nearestPromptId);
    }
};

const initializePromptRail = () => {
    if (!chatMessages || !promptRailNodes || !promptRail) return;
    promptAnchors = Array.from(chatMessages.querySelectorAll("[data-prompt-id]"));
    if (!promptAnchors.length) {
        promptRail.classList.add("hidden");
        return;
    }

    promptRail.classList.remove("hidden");
    promptRailNodes.innerHTML = "";
    promptNodeButtons.clear();

    promptAnchors.forEach((anchor, index) => {
        const promptId = anchor.dataset.promptId || `${index + 1}`;
        const promptText = anchor.querySelector(".bg-brand-100")?.textContent?.trim() || `Prompt ${index + 1}`;
        const nodeButton = document.createElement("button");
        nodeButton.type = "button";
        nodeButton.className = "prompt-rail-node";
        nodeButton.textContent = `${index + 1}`;
        nodeButton.dataset.promptId = promptId;
        nodeButton.title = promptText;
        nodeButton.setAttribute("aria-label", `Jump to prompt ${index + 1}`);
        nodeButton.addEventListener("click", () => {
            anchor.scrollIntoView({ behavior: "smooth", block: "center" });
            setActivePromptNode(promptId);
        });
        promptRailNodes.appendChild(nodeButton);
        promptNodeButtons.set(promptId, nodeButton);
    });

    chatMessages.addEventListener("scroll", () => {
        if (promptScrollTicking) return;
        promptScrollTicking = true;
        window.requestAnimationFrame(() => {
            syncActivePromptNodeFromScroll();
            promptScrollTicking = false;
        });
    }, { passive: true });

    updatePromptRailDockSide();
    syncActivePromptNodeFromScroll();
};

const updateConversationOffset = () => {
    updatePromptRailDockSide();

    if (isPanelCollapsed) {
        chatMain.style.paddingLeft = "0px";
        chatMain.style.paddingRight = "0px";
        return;
    }

    const reserved = getCurrentPanelWidth() + (PANEL_GUTTER * 3);
    if (dockSide === "left") {
        chatMain.style.paddingLeft = `${reserved}px`;
        chatMain.style.paddingRight = "0px";
        resizeHandle.classList.remove("left-0", "-translate-x-1/2");
        resizeHandle.classList.add("right-0", "translate-x-1/2");
    } else {
        chatMain.style.paddingRight = `${reserved}px`;
        chatMain.style.paddingLeft = "0px";
        resizeHandle.classList.remove("right-0", "translate-x-1/2");
        resizeHandle.classList.add("left-0", "-translate-x-1/2");
    }
};
    
const snapPanelToSide = (side) => {
    const workspaceRect = workspace.getBoundingClientRect();
    const panelRect = panel.getBoundingClientRect();
    const panelWidth = getCurrentPanelWidth();
    const snappedLeft = side === "right"
        ? Math.max(PANEL_GUTTER, workspaceRect.width - panelWidth - PANEL_GUTTER)
        : PANEL_GUTTER;
    panel.style.left = `${snappedLeft}px`;
    panel.style.top = `${PANEL_GUTTER}px`;
    updateConversationOffset();
};

document.querySelectorAll("[data-section-toggle]").forEach((button) => {
    button.addEventListener("click", () => {
        const sectionName = button.dataset.sectionToggle;
        const section = sectionName === "sources" ? sourcesSection : studyAidSection;
        const isOpen = section.dataset.open === "true";
        setSectionState(sectionName, !isOpen);
    });
});

collapseButton.addEventListener("click", () => {
    isPanelCollapsed = !isPanelCollapsed;
    if (isPanelCollapsed) {
        expandedPanelWidth = panel.offsetWidth;
    }
    applyPanelCollapseState();
    snapPanelToSide(dockSide);
    savePanelState();
});

dragHandle.addEventListener("mousedown", (event) => {
    if (event.target.closest("button")) return;
    dragging = true;
    panel.classList.remove("duration-200");
    const panelRect = panel.getBoundingClientRect();
    dragOffsetX = event.clientX - panelRect.left;
});
    
resizeHandle.addEventListener("mousedown", (event) => {
    event.preventDefault();
    if (isPanelCollapsed || isToolboxDocumentMode) return;
    resizing = true;
    panel.classList.remove("duration-200");
    resizeStartX = event.clientX;
    resizeStartWidth = panel.offsetWidth;
});

window.addEventListener("mousemove", (event) => {
    if (dragging) {
        const workspaceRect = workspace.getBoundingClientRect();
        const panelRect = panel.getBoundingClientRect();
        const nextLeft = clamp(event.clientX - workspaceRect.left - dragOffsetX, PANEL_GUTTER, workspaceRect.width - panelRect.width - PANEL_GUTTER);
        panel.style.left = `${nextLeft}px`;
        panel.style.top = `${PANEL_GUTTER}px`;
        return;
    }

    if (resizing) {
        if (isToolboxDocumentMode) return;
        const workspaceWidth = workspace.clientWidth;
        const delta = event.clientX - resizeStartX;
        const nextWidth = dockSide === "left" ? resizeStartWidth + delta : resizeStartWidth - delta;
        const maxAllowed = Math.min(PANEL_MAX_WIDTH, workspaceWidth - 120);
        const width = clamp(nextWidth, PANEL_MIN_WIDTH, maxAllowed);
        expandedPanelWidth = width;
        panel.style.width = `${width}px`;
        snapPanelToSide(dockSide);
    }
});

window.addEventListener("mouseup", () => {
    if (dragging) {
        const panelMid = panel.offsetLeft + (panel.offsetWidth / 2);
        const workspaceMid = workspace.clientWidth / 2;
        dockSide = panelMid >= workspaceMid ? "right" : "left";
        snapPanelToSide(dockSide);
        savePanelState();
    }
    if (resizing) {
        snapPanelToSide(dockSide);
        savePanelState();
    }
    panel.classList.add("duration-200");
    dragging = false;
    resizing = false;
});
    
window.addEventListener("resize", () => {
    if (isToolboxDocumentMode) {
        expandedPanelWidth = clamp(getMaxAllowedPanelWidth(), PANEL_MIN_WIDTH, PANEL_MAX_WIDTH);
        panel.style.width = `${expandedPanelWidth}px`;
    }
    snapPanelToSide(dockSide);
});

loadPanelState();
initializeSourceSelectionButtons();
initializeDocumentViewer();
initializePromptRail();
applyPanelCollapseState();
updateSectionLayout();
snapPanelToSide(dockSide);
window.requestAnimationFrame(() => snapPanelToSide(dockSide));
window.addEventListener("load", () => snapPanelToSide(dockSide));
