// ============================================================================
// CHAT PAGE PANEL STATE AND INTERACTIONS
// ============================================================================
// This module owns the floating tools panel: persisted layout state, collapse
// behavior, docking, drag, resize, and section visibility.
(function initializeChatPanelModule() {
    const ns = window.ChatPage;
    if (!ns) return;

    const {
        workspace,
        panel,
        chatMain,
        dragHandle,
        panelTitle,
        toolsMenu,
        collapseButton,
        resizeHandle,
        sourcesSection,
        studyAidSection,
        sourcesHeader,
        studyAidHeader,
        sourcesAddBtn,
        sourcesSelectAllBtn,
        sourcesBody,
        studyAidBody,
        sourcesDetailedList,
        studyAidDetailedList,
        sourcesIconList,
        studyAidIconList,
        toolsDefaultContent,
        toolsDocumentContent,
    } = ns.elements;
    const state = ns.state;
    const constants = ns.constants;

    ns.savePanelState = () => {
        const payload = {
            collapsed: state.isPanelCollapsed,
            width: state.expandedPanelWidth,
            side: state.dockSide,
        };
        localStorage.setItem(constants.PANEL_STORAGE_KEY, JSON.stringify(payload));
    };

    ns.loadPanelState = () => {
        try {
            const raw = localStorage.getItem(constants.PANEL_STORAGE_KEY);
            if (!raw) return;
            const parsed = JSON.parse(raw);
            if (typeof parsed.collapsed === "boolean") {
                state.isPanelCollapsed = parsed.collapsed;
            }
            if (typeof parsed.width === "number") {
                state.expandedPanelWidth = ns.clamp(parsed.width, constants.PANEL_MIN_WIDTH, constants.PANEL_MAX_WIDTH);
            }
            if (parsed.side === "left" || parsed.side === "right") {
                state.dockSide = parsed.side;
            }
        } catch (_error) {
            // Ignore invalid persisted state and fallback to defaults.
        }
    };

    ns.updateSectionLayout = () => {
        if (state.isPanelCollapsed) {
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

    ns.applyPanelCollapseState = () => {
        // Document mode replaces the normal panel chrome with the preview UI.
        dragHandle.classList.toggle("hidden", state.isToolboxDocumentMode);

        if (state.isPanelCollapsed && state.isToolboxDocumentMode) {
            ns.closeToolboxDocument?.();
        }

        if (!state.isToolboxDocumentMode) {
            panelTitle.classList.toggle("hidden", state.isPanelCollapsed);
            toolsMenu.classList.toggle("hidden", state.isPanelCollapsed);
            dragHandle.classList.toggle("justify-center", state.isPanelCollapsed);
            dragHandle.classList.toggle("justify-between", !state.isPanelCollapsed);
        }

        sourcesHeader.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        studyAidHeader.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        sourcesAddBtn.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        sourcesSelectAllBtn.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        sourcesDetailedList.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        studyAidDetailedList.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        sourcesIconList.classList.toggle("hidden", !state.isPanelCollapsed || state.isToolboxDocumentMode);
        studyAidIconList.classList.toggle("hidden", !state.isPanelCollapsed || state.isToolboxDocumentMode);

        if (toolsDefaultContent) {
            toolsDefaultContent.style.display = state.isToolboxDocumentMode ? "none" : "flex";
        }
        if (toolsDocumentContent) {
            const showDoc = state.isToolboxDocumentMode && !state.isPanelCollapsed;
            toolsDocumentContent.style.display = showDoc ? "flex" : "none";
        }

        resizeHandle.classList.toggle("hidden", state.isPanelCollapsed || state.isToolboxDocumentMode);
        panel.style.width = state.isPanelCollapsed
            ? `${constants.PANEL_COLLAPSED_WIDTH}px`
            : `${state.expandedPanelWidth}px`;
        ns.updateSectionLayout();
    };

    ns.setSectionState = (sectionName, isOpen) => {
        if (state.isPanelCollapsed) return;
        const section = sectionName === "sources" ? sourcesSection : studyAidSection;
        section.dataset.open = isOpen ? "true" : "false";
        const toggleIcon = document.querySelector(`[data-chevron-icon="${sectionName}"]`);
        toggleIcon.classList.toggle("fa-chevron-up", isOpen);
        toggleIcon.classList.toggle("fa-chevron-down", !isOpen);
        ns.updateSectionLayout();
    };

    ns.updateConversationOffset = () => {
        ns.updatePromptRailDockSide?.();

        if (state.isPanelCollapsed) {
            chatMain.style.paddingLeft = "0px";
            chatMain.style.paddingRight = "0px";
            return;
        }

        const reserved = ns.getCurrentPanelWidth() + (constants.PANEL_GUTTER * 3);
        if (state.dockSide === "left") {
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

    ns.snapPanelToSide = (side) => {
        const workspaceRect = workspace.getBoundingClientRect();
        const panelWidth = ns.getCurrentPanelWidth();
        const snappedLeft = side === "right"
            ? Math.max(constants.PANEL_GUTTER, workspaceRect.width - panelWidth - constants.PANEL_GUTTER)
            : constants.PANEL_GUTTER;

        panel.style.left = `${snappedLeft}px`;
        panel.style.top = `${constants.PANEL_GUTTER}px`;
        ns.updateConversationOffset();
    };

    ns.bindSectionToggles = () => {
        document.querySelectorAll("[data-section-toggle]").forEach((button) => {
            button.addEventListener("click", () => {
                const sectionName = button.dataset.sectionToggle;
                const section = sectionName === "sources" ? sourcesSection : studyAidSection;
                const isOpen = section.dataset.open === "true";
                ns.setSectionState(sectionName, !isOpen);
            });
        });
    };

    ns.bindPanelInteractions = () => {
        collapseButton.addEventListener("click", () => {
            state.isPanelCollapsed = !state.isPanelCollapsed;
            if (state.isPanelCollapsed) {
                state.expandedPanelWidth = panel.offsetWidth;
            }
            ns.applyPanelCollapseState();
            ns.snapPanelToSide(state.dockSide);
            ns.savePanelState();
        });

        dragHandle.addEventListener("mousedown", (event) => {
            if (event.target.closest("button")) return;
            state.dragging = true;
            panel.classList.remove("duration-200");
            const panelRect = panel.getBoundingClientRect();
            state.dragOffsetX = event.clientX - panelRect.left;
        });

        resizeHandle.addEventListener("mousedown", (event) => {
            event.preventDefault();
            if (state.isPanelCollapsed || state.isToolboxDocumentMode) return;
            state.resizing = true;
            panel.classList.remove("duration-200");
            state.resizeStartX = event.clientX;
            state.resizeStartWidth = panel.offsetWidth;
        });

        window.addEventListener("mousemove", (event) => {
            if (state.dragging) {
                const workspaceRect = workspace.getBoundingClientRect();
                const panelRect = panel.getBoundingClientRect();
                const nextLeft = ns.clamp(
                    event.clientX - workspaceRect.left - state.dragOffsetX,
                    constants.PANEL_GUTTER,
                    workspaceRect.width - panelRect.width - constants.PANEL_GUTTER,
                );
                panel.style.left = `${nextLeft}px`;
                panel.style.top = `${constants.PANEL_GUTTER}px`;
                return;
            }

            if (state.resizing) {
                if (state.isToolboxDocumentMode) return;
                const workspaceWidth = workspace.clientWidth;
                const delta = event.clientX - state.resizeStartX;
                const nextWidth = state.dockSide === "left"
                    ? state.resizeStartWidth + delta
                    : state.resizeStartWidth - delta;
                const maxAllowed = Math.min(constants.PANEL_MAX_WIDTH, workspaceWidth - 120);
                const width = ns.clamp(nextWidth, constants.PANEL_MIN_WIDTH, maxAllowed);
                state.expandedPanelWidth = width;
                panel.style.width = `${width}px`;
                ns.snapPanelToSide(state.dockSide);
            }
        });

        window.addEventListener("mouseup", () => {
            if (state.dragging) {
                const panelMid = panel.offsetLeft + (panel.offsetWidth / 2);
                const workspaceMid = workspace.clientWidth / 2;
                state.dockSide = panelMid >= workspaceMid ? "right" : "left";
                ns.snapPanelToSide(state.dockSide);
                ns.savePanelState();
            }

            if (state.resizing) {
                ns.snapPanelToSide(state.dockSide);
                ns.savePanelState();
            }

            panel.classList.add("duration-200");
            state.dragging = false;
            state.resizing = false;
        });

        window.addEventListener("resize", () => {
            if (state.isToolboxDocumentMode) {
                state.expandedPanelWidth = ns.clamp(
                    ns.getMaxAllowedPanelWidth(),
                    constants.PANEL_MIN_WIDTH,
                    constants.PANEL_MAX_WIDTH,
                );
                panel.style.width = `${state.expandedPanelWidth}px`;
            }
            ns.snapPanelToSide(state.dockSide);
        });
    };
}());
