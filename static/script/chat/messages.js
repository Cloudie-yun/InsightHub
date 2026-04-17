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
        promptInput,
        sendButton,
    } = ns.elements;
    const state = ns.state;

    ns.scrollMessagesToBottom = () => {
        if (!chatMessages) return;
        chatMessages.scrollTop = chatMessages.scrollHeight;
    };

    ns.getNextPromptId = () => {
        state.nextPromptIndex += 1;
        return `prompt-${state.nextPromptIndex}`;
    };

    ns.bootstrapPromptIndex = () => {
        if (!chatMessages) return;
        const promptAnchors = Array.from(chatMessages.querySelectorAll("[data-prompt-id]"));
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

        const bubble = document.createElement("div");
        bubble.className = "max-w-3xl rounded-3xl rounded-tr-none bg-brand-600 px-5 py-4 text-white shadow-sm";

        const label = document.createElement("div");
        label.className = "mb-2 flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-brand-100";
        label.textContent = "You";

        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap text-sm leading-7 text-white";
        content.textContent = messagePayload.message_text || "";

        bubble.appendChild(label);
        bubble.appendChild(content);
        article.appendChild(bubble);
        return article;
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
        wrapper.className = "rounded-xl border border-slate-200 bg-white px-3 py-3";

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
        badges.className = "mt-2 flex flex-wrap gap-2";
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
        reason.className = "mt-2 text-xs font-medium uppercase tracking-[0.14em] text-slate-400";
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
        article.className = "flex justify-start";
        article.dataset.messageId = messagePayload.message_id || "";
        article.dataset.messageRole = "assistant";

        const retrievalPayload = messagePayload.retrieval_payload || {};
        const filterSummary = retrievalPayload.filter_summary || {};
        const results = Array.isArray(retrievalPayload.results) ? retrievalPayload.results : [];

        const container = document.createElement("div");
        container.className = "w-full max-w-4xl";

        const summary = document.createElement("div");
        summary.className = "rounded-3xl border border-gray-200 bg-white px-5 py-4 shadow-sm";

        const label = document.createElement("div");
        label.className = "mb-2 flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400";
        label.textContent = "Retrieval Inspection";

        const content = document.createElement("div");
        content.className = "text-sm leading-6 text-slate-700";
        content.textContent = messagePayload.message_text || "Retrieval completed.";

        summary.appendChild(label);
        summary.appendChild(content);
        container.appendChild(summary);

        const diagnostics = document.createElement("div");
        diagnostics.className = "mt-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3";

        const diagnosticsLabel = document.createElement("p");
        diagnosticsLabel.className = "text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400";
        diagnosticsLabel.textContent = "Diagnostics";

        const diagnosticsGrid = document.createElement("div");
        diagnosticsGrid.className = "mt-2 grid gap-2 sm:grid-cols-4";
        diagnosticsGrid.appendChild(ns.createMetricCard("Top K", String(retrievalPayload.k || 0)));
        diagnosticsGrid.appendChild(ns.createMetricCard("Returned", String(retrievalPayload.returned_count || results.length)));
        diagnosticsGrid.appendChild(ns.createMetricCard("Filtered Out", String(filterSummary.excluded_candidate_count || 0)));
        diagnosticsGrid.appendChild(ns.createMetricCard(
            "Strategy",
            String(retrievalPayload.strategy || "vector").replace(/_/g, " "),
        ));

        diagnostics.appendChild(diagnosticsLabel);
        diagnostics.appendChild(diagnosticsGrid);
        container.appendChild(diagnostics);

        const resultsWrap = document.createElement("div");
        resultsWrap.className = "mt-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3";

        const resultsLabel = document.createElement("p");
        resultsLabel.className = "text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400";
        resultsLabel.textContent = "Top Chunks";
        resultsWrap.appendChild(resultsLabel);

        if (results.length) {
            const resultsList = document.createElement("div");
            resultsList.className = "mt-2 space-y-2";
            results.forEach((result, index) => {
                resultsList.appendChild(ns.createRetrievalResultNode(result, index));
            });
            resultsWrap.appendChild(resultsList);
        } else {
            const emptyState = document.createElement("div");
            emptyState.className = "mt-2 rounded-xl border border-dashed border-slate-200 bg-white px-4 py-5 text-sm text-slate-500";
            emptyState.textContent = "No useful chunks were found for this query after filtering.";
            resultsWrap.appendChild(emptyState);
        }

        container.appendChild(resultsWrap);
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
        if (!chatMessages) return;
        messageNodes.forEach((node) => {
            chatMessages.appendChild(node);
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
                ns.appendMessageNodes([ns.createRetrievalInspectionNode(payload.messages.assistant)]);
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
        if (!chatMessages || !promptInput || !sendButton) return;

        ns.bootstrapPromptIndex();
        ns.updateSendButtonState?.();
        ns.scrollMessagesToBottom();

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
    };
}());
