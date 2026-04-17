// ============================================================================
// CHAT PAGE MESSAGES
// ============================================================================
// This module owns query submission and renders retrieval inspection cards in
// the conversation thread.
(function initializeChatMessagesModule() {
    const ns = window.ChatPage;
    if (!ns) return;

    const {
        chatMessages,
        chatMessageList,
        scrollBottomButton,
        promptInput,
        sendButton,
    } = ns.elements;
    const state = ns.state;

    ns.updateScrollToBottomButton = () => {
        if (!chatMessages || !scrollBottomButton) return;
        const distanceFromBottom = chatMessages.scrollHeight - chatMessages.clientHeight - chatMessages.scrollTop;
        const shouldShow = distanceFromBottom > 160;
        scrollBottomButton.classList.toggle("hidden", !shouldShow);
        scrollBottomButton.classList.toggle("flex", shouldShow);
    };

    ns.scrollMessagesToBottom = () => {
        if (!chatMessages) return;
        chatMessages.scrollTop = chatMessages.scrollHeight;
        ns.updateScrollToBottomButton?.();
    };

    ns.getNextPromptId = () => {
        state.nextPromptIndex += 1;
        return `prompt-${state.nextPromptIndex}`;
    };

    ns.bootstrapPromptIndex = () => {
        if (!chatMessageList) return;
        const promptAnchors = Array.from(chatMessageList.querySelectorAll("[data-prompt-id]"));
        state.nextPromptIndex = promptAnchors.length;
    };

    ns.removeChatEmptyState = () => {
        document.getElementById("chat-empty-state")?.remove();
    };

    ns.createUserMessageNode = (messagePayload, promptId = ns.getNextPromptId()) => {
        const article = document.createElement("article");
        article.className = "flex justify-end";
        article.dataset.promptId = promptId;
        article.dataset.messageId = messagePayload.message_id || "";
        article.dataset.messageRole = "user";

        const wrapper = document.createElement("div");
        wrapper.className = "relative group max-w-xl pb-7";

        const bubble = document.createElement("div");
        bubble.className = "rounded-2xl rounded-tr-none bg-brand-100 px-4 py-2.5 text-[14px] font-normal leading-6 text-gray-800 shadow-sm";

        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap";
        content.textContent = messagePayload.message_text || "";

        bubble.appendChild(content);
        wrapper.appendChild(bubble);
        wrapper.appendChild(ns.createUserActionRow());
        article.appendChild(wrapper);
        return article;
    };

    ns.createIconButton = ({ title, iconClass, extraClass = "" }) => {
        const button = document.createElement("button");
        button.type = "button";
        button.title = title;
        button.className = `flex h-8 w-8 items-center justify-center rounded-md text-gray-400 transition-colors ${extraClass}`.trim();
        const icon = document.createElement("i");
        icon.className = iconClass;
        button.appendChild(icon);
        return button;
    };

    ns.createUserActionRow = () => {
        const actions = document.createElement("div");
        actions.className = "absolute bottom-0 right-1 flex items-center gap-1 opacity-0 pointer-events-none transition-opacity duration-150 group-hover:opacity-100 group-hover:pointer-events-auto";
        actions.appendChild(ns.createIconButton({
            title: "Copy",
            iconClass: "fa-solid fa-clone",
            extraClass: "hover:bg-gray-100 hover:text-gray-600",
        }));
        actions.appendChild(ns.createIconButton({
            title: "Edit",
            iconClass: "fa-solid fa-pen",
            extraClass: "hover:bg-brand-50 hover:text-brand-600",
        }));
        return actions;
    };

    ns.createAssistantActionRow = () => {
        const actions = document.createElement("div");
        actions.className = "mt-1.5 flex items-center gap-1";
        actions.appendChild(ns.createIconButton({
            title: "Copy",
            iconClass: "fa-solid fa-clone",
            extraClass: "hover:bg-gray-100 hover:text-gray-600",
        }));
        actions.appendChild(ns.createIconButton({
            title: "Helpful",
            iconClass: "fa-regular fa-thumbs-up",
            extraClass: "hover:bg-green-50 hover:text-green-600",
        }));
        actions.appendChild(ns.createIconButton({
            title: "Not helpful",
            iconClass: "fa-regular fa-thumbs-down",
            extraClass: "hover:bg-red-50 hover:text-red-600",
        }));
        actions.appendChild(ns.createIconButton({
            title: "Regenerate",
            iconClass: "fa-solid fa-rotate-right",
            extraClass: "hover:bg-gray-100 hover:text-gray-600",
        }));
        actions.appendChild(ns.createIconButton({
            title: "Create study aid",
            iconClass: "fa-solid fa-lightbulb",
            extraClass: "hover:bg-amber-50 hover:text-amber-600",
        }));
        return actions;
    };

    ns.createMetricCard = (labelText, valueText) => {
        const card = document.createElement("div");
        card.className = "rounded-xl border border-slate-200 bg-white px-3 py-2";

        const label = document.createElement("p");
        label.className = "text-[11px] uppercase tracking-[0.18em] text-slate-400";
        label.textContent = labelText;

        const value = document.createElement("p");
        value.className = "mt-1 text-sm font-semibold text-slate-700";
        value.textContent = valueText;

        card.appendChild(label);
        card.appendChild(value);
        return card;
    };

    ns.createResultBadge = (text, className) => {
        const badge = document.createElement("span");
        badge.className = className;
        badge.textContent = text;
        return badge;
    };

    ns.getResultPageHint = (result) => {
        const sourceMetadata = result?.source_metadata || {};
        const pageValue = sourceMetadata.page || sourceMetadata.page_number || sourceMetadata.page_index;
        if (pageValue === undefined || pageValue === null || pageValue === "") {
            return "";
        }
        return `Page ${pageValue}`;
    };

    ns.createRetrievalResultNode = (result, index) => {
        const wrapper = document.createElement("div");
        wrapper.className = "rounded-2xl border border-slate-200 bg-slate-50/85 px-3.5 py-3";

        const header = document.createElement("div");
        header.className = "flex flex-wrap items-center justify-between gap-3";

        const title = document.createElement("p");
        title.className = "truncate text-sm font-medium text-slate-700";
        title.textContent = `#${index + 1} ${result.document_name || result.document_id || "Source"}`;

        const score = document.createElement("span");
        score.className = "text-xs font-semibold text-slate-500";
        score.textContent = `Score ${Number(result.score || 0).toFixed(3)}`;

        header.appendChild(title);
        header.appendChild(score);

        const badges = document.createElement("div");
        badges.className = "mt-1.5 flex flex-wrap gap-1.5";
        badges.appendChild(ns.createResultBadge(
            result.block_type || "block",
            "inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600",
        ));

        if (result.text_role) {
            badges.appendChild(ns.createResultBadge(
                result.text_role,
                "inline-flex items-center rounded-full bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700",
            ));
        }

        if (Array.isArray(result.section_path) && result.section_path.length) {
            badges.appendChild(ns.createResultBadge(
                result.section_path.join(" > "),
                "inline-flex items-center rounded-full bg-emerald-50 px-2 py-0.5 text-[11px] font-medium text-emerald-700",
            ));
        }

        const pageHint = ns.getResultPageHint(result);
        if (pageHint) {
            badges.appendChild(ns.createResultBadge(
                pageHint,
                "inline-flex items-center rounded-full bg-violet-50 px-2 py-0.5 text-[11px] font-medium text-violet-700",
            ));
        }

        const reason = document.createElement("p");
        reason.className = "mt-1.5 text-xs font-medium uppercase tracking-[0.14em] text-slate-400";
        reason.textContent = result.relevance_reason || "Matched retrieval chunk";

        const snippet = document.createElement("p");
        snippet.className = "mt-1 text-xs leading-5 text-slate-500";
        snippet.textContent = result.snippet || "";

        wrapper.appendChild(header);
        wrapper.appendChild(badges);
        wrapper.appendChild(reason);
        wrapper.appendChild(snippet);
        return wrapper;
    };

    ns.createRetrievalInspectionNode = (messagePayload) => {
        const article = document.createElement("article");
        article.className = "flex justify-center";
        article.dataset.messageId = messagePayload.message_id || "";
        article.dataset.messageRole = "assistant";

        const retrievalPayload = messagePayload.retrieval_payload || {};
        const filterSummary = retrievalPayload.filter_summary || {};
        const results = Array.isArray(retrievalPayload.results) ? retrievalPayload.results : [];

        const container = document.createElement("div");
        container.className = "w-full max-w-4xl";

        const summary = document.createElement("div");
        summary.className = "px-1 py-0.5 text-[14px] leading-6 text-gray-800";

        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap";
        content.textContent = messagePayload.message_text || "Retrieval completed.";

        summary.appendChild(content);
        container.appendChild(summary);

        const diagnostics = document.createElement("div");
        diagnostics.className = "mt-3 rounded-3xl border border-slate-200/80 bg-white/90 px-4 py-3 shadow-sm";

        const diagnosticsHeader = document.createElement("div");
        diagnosticsHeader.className = "flex flex-wrap items-center justify-between gap-3";

        const diagnosticsLabelWrap = document.createElement("div");
        const diagnosticsLabel = document.createElement("p");
        diagnosticsLabel.className = "text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400";
        diagnosticsLabel.textContent = "Sources";
        const diagnosticsHelp = document.createElement("p");
        diagnosticsHelp.className = "mt-1 text-[13px] text-slate-500";
        diagnosticsHelp.textContent = "Top ranked chunks from the selected documents.";
        diagnosticsLabelWrap.appendChild(diagnosticsLabel);
        diagnosticsLabelWrap.appendChild(diagnosticsHelp);

        const diagnosticsGrid = document.createElement("div");
        diagnosticsGrid.className = "flex flex-wrap items-center gap-2 text-xs text-slate-500";
        diagnosticsGrid.appendChild(ns.createResultBadge(
            `Top K ${String(retrievalPayload.k || 0)}`,
            "inline-flex items-center rounded-full bg-slate-100 px-2.5 py-0.5 font-medium",
        ));
        diagnosticsGrid.appendChild(ns.createResultBadge(
            `Returned ${String(retrievalPayload.returned_count || results.length)}`,
            "inline-flex items-center rounded-full bg-slate-100 px-2.5 py-0.5 font-medium",
        ));
        diagnosticsGrid.appendChild(ns.createResultBadge(
            `Filtered ${String(filterSummary.excluded_candidate_count || 0)}`,
            "inline-flex items-center rounded-full bg-slate-100 px-2.5 py-0.5 font-medium",
        ));
        diagnosticsGrid.appendChild(ns.createResultBadge(
            String(retrievalPayload.strategy || "vector").replace(/_/g, " "),
            "inline-flex items-center rounded-full bg-brand-50 px-2.5 py-0.5 font-medium capitalize text-brand-700",
        ));

        diagnosticsHeader.appendChild(diagnosticsLabelWrap);
        diagnosticsHeader.appendChild(diagnosticsGrid);
        diagnostics.appendChild(diagnosticsHeader);
        container.appendChild(diagnostics);

        const resultsWrap = document.createElement("div");
        resultsWrap.className = "mt-4";

        if (results.length) {
            const resultsList = document.createElement("div");
            resultsList.className = "space-y-2.5";
            results.forEach((result, index) => {
                resultsList.appendChild(ns.createRetrievalResultNode(result, index));
            });
            resultsWrap.appendChild(resultsList);
        } else {
            const emptyState = document.createElement("div");
            emptyState.className = "rounded-2xl border border-dashed border-slate-200 bg-slate-50/85 px-4 py-4 text-sm text-slate-500";
            emptyState.textContent = "No useful chunks were found for this query after filtering.";
            resultsWrap.appendChild(emptyState);
        }

        container.appendChild(resultsWrap);
        container.appendChild(ns.createAssistantActionRow());
        article.appendChild(container);
        return article;
    };

    ns.createAssistantFallbackNode = (messagePayload) => {
        const article = document.createElement("article");
        article.className = "flex justify-center";
        article.dataset.messageId = messagePayload?.message_id || "";
        article.dataset.messageRole = "assistant";

        const container = document.createElement("div");
        container.className = "w-full max-w-4xl";

        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap px-1 py-0.5 text-[14px] leading-6 text-gray-800";
        content.textContent = messagePayload?.message_text || "Retrieval completed.";

        container.appendChild(content);
        container.appendChild(ns.createAssistantActionRow());
        article.appendChild(container);
        return article;
    };

    ns.setMessageSendingState = (isSending) => {
        state.isSendingMessage = Boolean(isSending);
        if (promptInput) {
            promptInput.disabled = state.isSendingMessage || !ns.getCurrentConversationId();
        }
        ns.updateSendButtonState?.();
    };

    ns.appendMessageNodes = (messageNodes) => {
        if (!chatMessageList || !chatMessages) return;
        messageNodes.forEach((node) => {
            chatMessageList.appendChild(node);
        });
        ns.initializePromptRail?.();
        ns.scrollMessagesToBottom();
    };

    ns.submitChatPrompt = async () => {
        const conversationId = ns.getCurrentConversationId();
        if (!conversationId) {
            ns.notify("warning", "Open a conversation first.");
            return;
        }

        const query = String(promptInput?.value || "").trim();
        if (!query) {
            ns.setChatSendStatus("Enter a question to continue.", true);
            return;
        }

        const selectedDocumentIds = ns.getSelectedSourceDocumentIds?.() || [];
        if (!selectedDocumentIds.length) {
            ns.setChatSendStatus("Select at least one document before asking a question.", true);
            ns.notify("warning", "Select at least one document first.");
            return;
        }

        ns.setChatSendStatus("");
        ns.removeChatEmptyState();
        ns.setMessageSendingState(true);

        const optimisticUserNode = ns.createUserMessageNode({ message_text: query });
        ns.appendMessageNodes([optimisticUserNode]);
        if (promptInput) {
            promptInput.value = "";
        }

        try {
            const response = await fetch(`/api/conversations/${encodeURIComponent(conversationId)}/messages`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    query,
                    document_ids: selectedDocumentIds,
                    include_filtered: false,
                }),
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) {
                throw new Error(payload.error || `Request failed with HTTP ${response.status}`);
            }

            if (payload.messages?.user) {
                optimisticUserNode.dataset.messageId = payload.messages.user.message_id || "";
            }
            if (payload.messages?.assistant) {
                let assistantNode = null;
                try {
                    assistantNode = ns.createRetrievalInspectionNode(payload.messages.assistant);
                } catch (renderError) {
                    console.error("Failed to render retrieval inspection node:", renderError);
                    assistantNode = ns.createAssistantFallbackNode(payload.messages.assistant);
                }
                if (assistantNode) {
                    ns.appendMessageNodes([assistantNode]);
                }
            }
            ns.setChatSendStatus("");
        } catch (error) {
            optimisticUserNode.remove();
            ns.setChatSendStatus(error.message || "Unable to send your message right now.", true);
            ns.notify("error", error.message || "Unable to send your message right now.");
        } finally {
            ns.setMessageSendingState(false);
            promptInput?.focus();
        }
    };

    ns.initializeChatMessages = () => {
        if (!chatMessages || !chatMessageList || !promptInput || !sendButton) return;

        ns.bootstrapPromptIndex();
        ns.updateSendButtonState?.();
        ns.scrollMessagesToBottom();

        chatMessages.addEventListener("scroll", () => {
            ns.updateScrollToBottomButton?.();
        }, { passive: true });

        scrollBottomButton?.addEventListener("click", () => {
            if (!chatMessages) return;
            chatMessages.scrollTo({
                top: chatMessages.scrollHeight,
                behavior: "smooth",
            });
        });

        sendButton.addEventListener("click", () => {
            if (state.isSendingMessage) return;
            ns.submitChatPrompt();
        });

        promptInput.addEventListener("keydown", (event) => {
            if (event.key !== "Enter" || event.shiftKey) return;
            event.preventDefault();
            if (state.isSendingMessage) return;
            ns.submitChatPrompt();
        });

        document.querySelectorAll(".suggestion-chip").forEach((button) => {
            button.addEventListener("click", () => {
                if (!promptInput || promptInput.disabled) return;
                promptInput.value = button.textContent?.trim() || "";
                promptInput.focus();
            });
        });

        ns.updateScrollToBottomButton?.();
    };
}());
