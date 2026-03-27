// ============================================================================
// CHAT PAGE BOOTSTRAP
// ============================================================================
// This file intentionally stays small. The chat page logic is now split into
// focused plain-script modules under `static/script/chat/`, while this entry
// point keeps page initialization in one predictable place.
(function bootstrapChatPage() {
    const ns = window.ChatPage;

    if (!ns) return;

    ns.initializeChatPage = () => {
        const {
            workspace,
            panel,
            chatMain,
            dragHandle,
            collapseButton,
            resizeHandle,
        } = ns.elements;

        // Guard to keep this script safe if included on non-chat pages.
        if (!workspace || !panel || !chatMain || !dragHandle || !collapseButton || !resizeHandle) {
            return;
        }

        ns.loadPanelState();
        ns.bindSectionToggles();
        ns.bindPanelInteractions();
        ns.initializeSourceSelectionButtons();
        ns.initializeSourceSearchAndSort();
        ns.initializeDocumentViewer();
        ns.initializeConversationUpload();
        ns.initializePendingSourcePolling();
        ns.initializePromptRail();
        ns.applyPanelCollapseState();
        ns.updateSectionLayout();
        ns.snapPanelToSide(ns.state.dockSide);
        window.requestAnimationFrame(() => ns.snapPanelToSide(ns.state.dockSide));
        window.addEventListener("load", () => ns.snapPanelToSide(ns.state.dockSide));
    };

    ns.initializeChatPage();
}());
