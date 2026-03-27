// ============================================================================
// CHAT PAGE PROMPT RAIL
// ============================================================================
// The prompt rail mirrors prompt anchors in the conversation and keeps the
// active node in sync with scroll position and panel docking.
(function initializeChatPromptRailModule() {
    const ns = window.ChatPage;
    if (!ns) return;

    const { chatMessages, promptRail, promptRailNodes } = ns.elements;
    const state = ns.state;
    const constants = ns.constants;

    ns.updatePromptRailDockSide = () => {
        if (!promptRail) return;
        promptRail.classList.remove("left-3", "right-3");
        promptRail.classList.add(state.dockSide === "right" ? "left-3" : "right-3");
    };

    ns.setActivePromptNode = (promptId) => {
        state.promptNodeButtons.forEach((button, id) => {
            const isActive = id === promptId;
            button.classList.toggle("active", isActive);
            if (isActive) {
                button.scrollIntoView({ block: "nearest" });
            }
        });
    };

    ns.syncActivePromptNodeFromScroll = () => {
        if (!chatMessages || !state.promptAnchors.length) return;

        const messagesRect = chatMessages.getBoundingClientRect();
        const railTargetY = messagesRect.top + Math.min(messagesRect.height * 0.35, 220);
        let nearestPromptId = state.promptAnchors[0]?.dataset.promptId || null;
        let nearestDistance = Number.POSITIVE_INFINITY;

        state.promptAnchors.forEach((anchor) => {
            const anchorDistance = Math.abs(anchor.getBoundingClientRect().top - railTargetY);
            if (anchorDistance < nearestDistance) {
                nearestDistance = anchorDistance;
                nearestPromptId = anchor.dataset.promptId;
            }
        });

        if (nearestPromptId) {
            ns.setActivePromptNode(nearestPromptId);
        }
    };

    ns.initializePromptRail = () => {
        if (!chatMessages || !promptRailNodes || !promptRail) return;

        state.promptAnchors = Array.from(chatMessages.querySelectorAll("[data-prompt-id]"));
        if (!state.promptAnchors.length) {
            promptRail.classList.add("hidden");
            return;
        }

        promptRail.classList.remove("hidden");
        promptRailNodes.innerHTML = "";
        state.promptNodeButtons.clear();

        state.promptAnchors.forEach((anchor, index) => {
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
                ns.setActivePromptNode(promptId);
            });
            promptRailNodes.appendChild(nodeButton);
            state.promptNodeButtons.set(promptId, nodeButton);
        });

        chatMessages.addEventListener("scroll", () => {
            if (state.promptScrollTicking) return;
            state.promptScrollTicking = true;
            window.requestAnimationFrame(() => {
                ns.syncActivePromptNodeFromScroll();
                state.promptScrollTicking = false;
            });
        }, { passive: true });

        ns.updatePromptRailDockSide();
        ns.syncActivePromptNodeFromScroll();
    };
}());
