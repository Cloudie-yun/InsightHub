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
        article.dataset.familyId = messagePayload.family_id || "";
        article.dataset.versionIndex = String(messagePayload.version_index || 1);
        article.dataset.versionCount = String(messagePayload.version_count || 1);

        const wrapper = document.createElement("div");
        wrapper.className = "relative group max-w-xl pb-7";

        const bubble = document.createElement("div");
        bubble.className = "rounded-2xl rounded-tr-none bg-brand-100 px-4 py-2.5 text-[14px] font-normal leading-6 text-gray-800 shadow-sm";

        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap";
        content.dataset.copyContent = "message";
        content.textContent = messagePayload.message_text || "";

        bubble.appendChild(content);
        wrapper.appendChild(bubble);
        if (Number(messagePayload.version_count || 1) > 1) {
            wrapper.appendChild(ns.createVersionControls(messagePayload, { absolute: true }));
        }
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
        const copyButton = ns.createIconButton({
            title: "Copy",
            iconClass: "fa-solid fa-clone",
            extraClass: "hover:bg-gray-100 hover:text-gray-600",
        });
        copyButton.dataset.copyMessage = "true";
        actions.appendChild(copyButton);
        const editButton = ns.createIconButton({
            title: "Edit",
            iconClass: "fa-solid fa-pen",
            extraClass: "hover:bg-brand-50 hover:text-brand-600",
        });
        editButton.dataset.editMessage = "true";
        actions.appendChild(editButton);
        return actions;
    };

    ns.createAssistantActionRow = () => {
        const actions = document.createElement("div");
        actions.className = "mt-1.5 flex items-center gap-1";
        const copyButton = ns.createIconButton({
            title: "Copy",
            iconClass: "fa-solid fa-clone",
            extraClass: "hover:bg-gray-100 hover:text-gray-600",
        });
        copyButton.dataset.copyMessage = "true";
        actions.appendChild(copyButton);
        const regenerateButton = ns.createIconButton({
            title: "Regenerate",
            iconClass: "fa-solid fa-rotate-right",
            extraClass: "hover:bg-gray-100 hover:text-gray-600",
        });
        regenerateButton.dataset.regenerateMessage = "true";
        actions.appendChild(regenerateButton);
        actions.appendChild(ns.createIconButton({
            title: "Create study aid",
            iconClass: "fa-solid fa-lightbulb",
            extraClass: "hover:bg-amber-50 hover:text-amber-600",
        }));
        return actions;
    };

    ns.createVersionControls = (messagePayload, options = {}) => {
        const current = Math.max(1, Number(messagePayload?.version_index || 1));
        const total = Math.max(1, Number(messagePayload?.version_count || 1));
        if (total <= 1 || !messagePayload?.family_id) {
            return null;
        }

        const wrap = document.createElement("div");
        wrap.className = options.absolute
            ? "absolute bottom-0 left-1 flex items-center gap-1 rounded-full bg-white/95 px-2 py-1 text-[11px] font-semibold text-slate-500 shadow-sm"
            : "mt-2 flex items-center gap-1 text-[11px] font-semibold text-slate-500";

        const addNavButton = (direction, targetVersion, disabled) => {
            const button = document.createElement("button");
            button.type = "button";
            button.dataset.messageVersionNav = direction;
            button.dataset.familyId = messagePayload.family_id || "";
            button.dataset.role = messagePayload.role || "";
            button.dataset.versionNumber = String(targetVersion);
            button.title = direction === "prev" ? "Previous version" : "Next version";
            button.className = `flex h-5 min-w-[10px] items-center justify-center rounded-sm px-0.5 transition-colors hover:bg-slate-100 ${disabled ? "pointer-events-none opacity-40" : ""}`.trim();
            button.textContent = direction === "prev" ? "<" : ">";
            wrap.appendChild(button);
        };

        addNavButton("prev", current - 1, current <= 1);
        const label = document.createElement("span");
        label.textContent = `${current}/${total}`;
        wrap.appendChild(label);
        addNavButton("next", current + 1, current >= total);
        return wrap;
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

    ns.parseJsonDataAttribute = (value, fallback) => {
        const normalized = String(value || "").trim();
        if (!normalized) return fallback;
        try {
            return JSON.parse(normalized);
        } catch (error) {
            return fallback;
        }
    };

    ns.buildCitationTargetUrl = (citation) => {
        const documentId = String(citation?.document_id || "").trim();
        if (!documentId) return "";

        const params = new URLSearchParams();
        const conversationId = String(ns.getCurrentConversationId?.() || window.__CURRENT_CONVERSATION_ID__ || "").trim();
        const anchor = citation?.anchor && typeof citation.anchor === "object" ? citation.anchor : {};
        if (conversationId) params.set("conversation_id", conversationId);
        if (anchor.page !== undefined && anchor.page !== null && String(anchor.page).trim()) {
            params.set("page", String(anchor.page).trim());
        }
        if (anchor.char_offset !== undefined && anchor.char_offset !== null && String(anchor.char_offset).trim()) {
            params.set("char_offset", String(anchor.char_offset).trim());
        }
        if (Array.isArray(anchor.section_path) && anchor.section_path.length) {
            params.set(
                "section_path",
                anchor.section_path.map((item) => String(item || "").trim()).filter(Boolean).join(" > "),
            );
        }

        const query = params.toString();
        return `/documents/${encodeURIComponent(documentId)}/parser-results${query ? `?${query}` : ""}`;
    };

    ns.openCitationAnchor = (citation) => {
        const targetUrl = ns.buildCitationTargetUrl(citation);
        if (!targetUrl) {
            ns.notify?.("warning", "This citation is missing a document link.");
            return;
        }
        window.location.href = targetUrl;
    };

    ns.createCitationTrigger = (citation, label, extraClass = "") => {
        const button = document.createElement("button");
        button.type = "button";
        button.dataset.openCitation = "true";
        button.dataset.citationPayload = JSON.stringify(citation || {});
        button.className = `inline-flex items-center rounded-md border border-brand-200 bg-brand-50 px-1.5 py-0.5 text-[11px] font-semibold text-brand-700 align-middle transition hover:border-brand-300 hover:bg-brand-100 ${extraClass}`.trim();
        button.textContent = label;
        if (citation?.snippet) {
            button.title = citation.snippet;
        }
        return button;
    };

    ns.createAnswerTextNode = (answerText, citationIndexMap = {}) => {
        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap";
        content.dataset.copyContent = "message";
        content.dataset.copyRaw = JSON.stringify(String(answerText || ""));

        const normalizedAnswer = String(answerText || "");
        const normalizedMap = citationIndexMap && typeof citationIndexMap === "object" ? citationIndexMap : {};
        const pattern = /\[(\d+)\]/g;
        let cursor = 0;
        let match = pattern.exec(normalizedAnswer);

        while (match) {
            if (match.index > cursor) {
                content.appendChild(document.createTextNode(normalizedAnswer.slice(cursor, match.index)));
            }

            const citationIndex = String(match[1] || "").trim();
            const citation = normalizedMap[citationIndex];
            if (citation && typeof citation === "object") {
                const label = citation.page_label ? `[${citation.page_label}]` : `[${citationIndex}]`;
                content.appendChild(ns.createCitationTrigger(citation, label, "mx-0.5"));
            } else {
                content.appendChild(document.createTextNode(match[0]));
            }

            cursor = pattern.lastIndex;
            match = pattern.exec(normalizedAnswer);
        }

        if (cursor < normalizedAnswer.length) {
            content.appendChild(document.createTextNode(normalizedAnswer.slice(cursor)));
        }

        return content;
    };

    ns.createCitationList = (citations) => {
        const normalizedCitations = Array.isArray(citations) ? citations : [];
        if (!normalizedCitations.length) return null;

        const details = document.createElement("details");
        details.className = "overflow-hidden rounded-2xl border border-slate-200/80 bg-white/85 shadow-sm";

        const summary = document.createElement("summary");
        summary.className = "flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3 text-sm font-semibold text-slate-700";
        summary.innerHTML = `<span>Source citations</span><span class="text-xs font-medium uppercase tracking-[0.14em] text-slate-400">${normalizedCitations.length} items</span>`;
        details.appendChild(summary);

        const body = document.createElement("div");
        body.className = "border-t border-slate-200/80 px-4 py-3";

        normalizedCitations.forEach((citation) => {
            const row = document.createElement("div");
            row.className = "flex flex-col gap-3 py-3 first:pt-0 last:pb-0 sm:flex-row sm:items-start sm:justify-between";

            const copy = document.createElement("div");
            copy.className = "min-w-0 flex-1";

            const title = document.createElement("p");
            title.className = "text-sm font-semibold text-slate-700";
            title.textContent = `[${citation?.index || "?"}] ${citation?.document_name || citation?.document_id || "Source"}${citation?.page_label ? `, ${citation.page_label}` : ""}`;
            copy.appendChild(title);

            if (citation?.snippet) {
                const snippet = document.createElement("p");
                snippet.className = "mt-1 text-sm leading-6 text-slate-500";
                snippet.textContent = `"...${String(citation.snippet).trim()}..."`;
                copy.appendChild(snippet);
            }

            const openButton = ns.createCitationTrigger(
                citation,
                "Open",
                "w-fit self-start px-3 py-1.5 text-xs",
            );
            row.appendChild(copy);
            row.appendChild(openButton);
            body.appendChild(row);
        });

        details.appendChild(body);
        return details;
    };

    ns.hydrateInitialAssistantAnswers = () => {
        document.querySelectorAll("[data-answer-text='true']").forEach((node) => {
            if (!(node instanceof HTMLElement) || node.dataset.answerHydrated === "true") return;
            const rawAnswer = ns.parseJsonDataAttribute(node.dataset.rawAnswer, node.textContent || "");
            const citationIndexMap = ns.parseJsonDataAttribute(node.dataset.citationIndexMap, {});
            const renderedNode = ns.createAnswerTextNode(rawAnswer, citationIndexMap);
            renderedNode.dataset.answerText = "true";
            renderedNode.dataset.rawAnswer = JSON.stringify(String(rawAnswer || ""));
            renderedNode.dataset.citationIndexMap = JSON.stringify(citationIndexMap || {});
            renderedNode.dataset.answerHydrated = "true";
            node.replaceWith(renderedNode);
        });

        document.querySelectorAll("[data-citation-list-host]").forEach((node) => {
            if (!(node instanceof HTMLElement) || node.dataset.citationListHydrated === "true") return;
            const citations = ns.parseJsonDataAttribute(node.dataset.citations, []);
            const list = ns.createCitationList(citations);
            if (list) {
                node.replaceChildren(list);
            }
            node.dataset.citationListHydrated = "true";
        });
    };

    ns.createLoadingDocumentChip = (documentLabel) => {
        const chip = document.createElement("span");
        chip.className = "inline-flex items-center gap-2 rounded-full border border-brand-200 bg-brand-50 px-3 py-1.5 text-xs font-medium text-brand-700 shadow-sm";

        const spinner = document.createElement("i");
        spinner.className = "fa-solid fa-spinner animate-spin text-[10px]";
        chip.appendChild(spinner);

        const text = document.createElement("span");
        text.textContent = documentLabel;
        chip.appendChild(text);
        return chip;
    };

    ns.getSelectedSourceDocumentLabels = () => {
        return Array.from(document.querySelectorAll('#sources-detailed-list [data-doc-id]'))
            .filter((node) => {
                const button = node.querySelector('[data-source-select-btn="true"]');
                return button?.dataset.selected === "true";
            })
            .map((node) => String(node.dataset.docTitle || node.dataset.docId || "Document").trim())
            .filter(Boolean);
    };

    ns.createAssistantLoadingNode = (documentLabels) => {
        const article = document.createElement("article");
        article.className = "flex justify-center";
        article.dataset.messageRole = "assistant-loading";

        const container = document.createElement("div");
        container.className = "w-full max-w-4xl px-1 py-1";

        const ellipsis = document.createElement("div");
        ellipsis.className = "chat-loading-ellipsis";
        ellipsis.setAttribute("aria-label", "Generating response");
        ellipsis.setAttribute("role", "status");

        for (let index = 0; index < 3; index += 1) {
            const dot = document.createElement("span");
            dot.className = "chat-loading-ellipsis-dot";
            ellipsis.appendChild(dot);
        }

        container.appendChild(ellipsis);

        article.appendChild(container);
        return article;
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
        article.dataset.familyId = messagePayload.family_id || "";
        article.dataset.versionIndex = String(messagePayload.version_index || 1);
        article.dataset.versionCount = String(messagePayload.version_count || 1);

        const retrievalPayload = messagePayload.retrieval_payload || {};
        const filterSummary = retrievalPayload.filter_summary || {};
        const results = Array.isArray(retrievalPayload.results) ? retrievalPayload.results : [];
        const citations = Array.isArray(messagePayload.citations)
            ? messagePayload.citations
            : (Array.isArray(retrievalPayload.citations) ? retrievalPayload.citations : []);
        const citationIndexMap = retrievalPayload?.citation_index_map && typeof retrievalPayload.citation_index_map === "object"
            ? retrievalPayload.citation_index_map
            : {};
        const confidence = String(
            messagePayload.confidence
            || retrievalPayload?.grounded_answer?.confidence
            || "",
        ).trim().toLowerCase();

        const container = document.createElement("div");
        container.className = "w-full max-w-4xl";

        const summary = document.createElement("div");
        summary.className = "px-1 py-0.5 text-[14px] leading-6 text-gray-800";

        const content = ns.createAnswerTextNode(
            messagePayload.message_text || "Retrieval completed.",
            citationIndexMap,
        );

        summary.appendChild(content);
        container.appendChild(summary);

        if (citations.length) {
            const citationsList = ns.createCitationList(citations);
            if (citationsList) {
                citationsList.classList.add("mt-3");
                container.appendChild(citationsList);
            }
        }

        if (confidence) {
            const confidenceWrap = document.createElement("div");
            confidenceWrap.className = "mt-3";
            const confidenceChip = document.createElement("span");
            confidenceChip.className = "inline-flex items-center rounded-full bg-slate-100 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500";
            confidenceChip.textContent = `Confidence ${confidence}`;
            confidenceWrap.appendChild(confidenceChip);
            container.appendChild(confidenceWrap);
        }

        const versionControls = ns.createVersionControls(messagePayload);
        if (versionControls) {
            container.appendChild(versionControls);
        }

        const diagnostics = document.createElement("div");
        diagnostics.className = "chat-retrieval-diagnostics mt-3 rounded-3xl border border-slate-200/80 bg-white/90 px-4 py-3 shadow-sm";
        diagnostics.dataset.troubleshootPanel = "retrieval";
        diagnostics.classList.toggle("hidden", !ns.isTroubleshootModeEnabled?.());

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

        diagnostics.appendChild(resultsWrap);
        container.appendChild(diagnostics);
        container.appendChild(ns.createAssistantActionRow());
        article.appendChild(container);
        return article;
    };

    ns.createAssistantFallbackNode = (messagePayload) => {
        const article = document.createElement("article");
        article.className = "flex justify-center";
        article.dataset.messageId = messagePayload?.message_id || "";
        article.dataset.messageRole = "assistant";
        article.dataset.familyId = messagePayload?.family_id || "";
        article.dataset.versionIndex = String(messagePayload?.version_index || 1);
        article.dataset.versionCount = String(messagePayload?.version_count || 1);

        const container = document.createElement("div");
        container.className = "w-full max-w-4xl";

        const content = document.createElement("div");
        content.className = "whitespace-pre-wrap px-1 py-0.5 text-[14px] leading-6 text-gray-800";
        content.dataset.copyContent = "message";
        content.textContent = messagePayload?.message_text || "Retrieval completed.";

        container.appendChild(content);
        const versionControls = ns.createVersionControls(messagePayload);
        if (versionControls) {
            container.appendChild(versionControls);
        }
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

    ns.renderConversationMessages = (messages) => {
        if (!chatMessageList) return;
        chatMessageList.innerHTML = "";
        state.nextPromptIndex = 0;

        (Array.isArray(messages) ? messages : []).forEach((messagePayload) => {
            const role = String(messagePayload?.role || "").trim().toLowerCase();
            let node = null;
            if (role === "user") {
                node = ns.createUserMessageNode(messagePayload);
            } else if (role === "assistant") {
                try {
                    node = ns.createRetrievalInspectionNode(messagePayload);
                } catch (error) {
                    console.error("Failed to render assistant message during rerender:", error);
                    node = ns.createAssistantFallbackNode(messagePayload);
                }
            }
            if (node) {
                chatMessageList.appendChild(node);
            }
        });

        ns.bootstrapPromptIndex?.();
        ns.initializePromptRail?.();
        ns.scrollMessagesToBottom();
    };

    ns.removeMessageTailFrom = (messageId) => {
        if (!chatMessageList || !messageId) return;
        let shouldRemove = false;
        Array.from(chatMessageList.children).forEach((node) => {
            if (!(node instanceof HTMLElement)) return;
            if (node.dataset.messageId === messageId) {
                shouldRemove = true;
            }
            if (shouldRemove) {
                node.remove();
            }
        });
        ns.bootstrapPromptIndex?.();
        ns.initializePromptRail?.();
        ns.updateScrollToBottomButton?.();
    };

    ns.beginMessageEdit = (messageId) => {
        if (!messageId || state.isSendingMessage) return;
        const messageNode = chatMessageList?.querySelector?.(`[data-message-id="${CSS.escape(messageId)}"][data-message-role="user"]`);
        const contentNode = messageNode?.querySelector?.("[data-copy-content='message']");
        const messageText = String(contentNode?.textContent || "").trim();
        if (!messageText) {
            ns.notify("warning", "Unable to load that message for editing.");
            return;
        }
        ns.closeInlineMessageEdit({ restoreOriginal: true });

        const bubble = contentNode?.closest("div.rounded-2xl");
        const actionRow = messageNode?.querySelector("[data-edit-message='true']")?.closest("div");
        if (!(bubble instanceof HTMLElement) || !(contentNode instanceof HTMLElement)) {
            ns.notify("warning", "Unable to open inline editor for that message.");
            return;
        }

        const editorWrap = document.createElement("div");
        editorWrap.className = "space-y-3";
        editorWrap.dataset.inlineEditPanel = "true";

        const textarea = document.createElement("textarea");
        textarea.className = "w-full resize-y rounded-2xl border border-slate-300 bg-white px-3 py-2 text-[15px] leading-8 text-slate-800 outline-none focus:border-brand-400 focus:ring-2 focus:ring-brand-200";
        textarea.rows = 7;
        textarea.value = messageText;
        textarea.dataset.inlineEditInput = "true";
        textarea.dataset.messageId = messageId;
        editorWrap.appendChild(textarea);

        const controls = document.createElement("div");
        controls.className = "flex items-center justify-end gap-2";

        const cancelBtn = document.createElement("button");
        cancelBtn.type = "button";
        cancelBtn.className = "rounded-full border border-slate-300 bg-white px-5 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-100";
        cancelBtn.textContent = "Cancel";
        cancelBtn.dataset.inlineEditCancel = "true";
        cancelBtn.dataset.messageId = messageId;
        controls.appendChild(cancelBtn);

        const sendBtn = document.createElement("button");
        sendBtn.type = "button";
        sendBtn.className = "rounded-full bg-slate-900 px-5 py-2 text-sm font-semibold text-white transition hover:bg-slate-800";
        sendBtn.textContent = "Send";
        sendBtn.dataset.inlineEditSend = "true";
        sendBtn.dataset.messageId = messageId;
        controls.appendChild(sendBtn);

        editorWrap.appendChild(controls);
        contentNode.replaceWith(editorWrap);
        if (actionRow instanceof HTMLElement) actionRow.classList.add("hidden");

        state.inlineMessageEdit = { messageId, originalText: messageText };
        textarea.focus();
        textarea.setSelectionRange(textarea.value.length, textarea.value.length);
        ns.setChatSendStatus("Editing in place. Send to regenerate from this turn.");
    };

    ns.closeInlineMessageEdit = (options = {}) => {
        const { restoreOriginal = false } = options;
        const activeMessageId = String(state.inlineMessageEdit?.messageId || "").trim();
        if (!activeMessageId || !chatMessageList) return;
        const messageNode = chatMessageList.querySelector(`[data-message-id="${CSS.escape(activeMessageId)}"][data-message-role="user"]`);
        const panel = messageNode?.querySelector("[data-inline-edit-panel='true']");
        const textarea = panel?.querySelector("[data-inline-edit-input='true']");
        if (!(messageNode instanceof HTMLElement) || !(panel instanceof HTMLElement) || !(textarea instanceof HTMLTextAreaElement)) {
            state.inlineMessageEdit = null;
            return;
        }

        const replacement = document.createElement("div");
        replacement.className = "whitespace-pre-wrap";
        replacement.dataset.copyContent = "message";
        const fallbackText = String(textarea.value || "").trim();
        replacement.textContent = restoreOriginal
            ? String(state.inlineMessageEdit?.originalText || fallbackText)
            : fallbackText;
        panel.replaceWith(replacement);

        const actionRow = messageNode.querySelector("[data-edit-message='true']")?.closest("div");
        if (actionRow instanceof HTMLElement) actionRow.classList.remove("hidden");

        state.inlineMessageEdit = null;
    };

    ns.regenerateAssistantMessage = async (messageId) => {
        const conversationId = ns.getCurrentConversationId();
        if (!conversationId || !messageId) return;
        if (state.isSendingMessage) return;

        const assistantLoadingNode = ns.createAssistantLoadingNode(ns.getSelectedSourceDocumentLabels());
        ns.appendMessageNodes([assistantLoadingNode]);
        ns.setMessageSendingState(true);
        ns.setChatSendStatus("");

        try {
            const response = await fetch(`/api/conversations/${encodeURIComponent(conversationId)}/messages`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    regenerate_message_id: messageId,
                    include_filtered: false,
                }),
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) {
                throw new Error(payload.error || `Request failed with HTTP ${response.status}`);
            }

            assistantLoadingNode.remove();
            if (Array.isArray(payload.conversation_messages)) {
                ns.renderConversationMessages(payload.conversation_messages);
            }
            ns.setChatSendStatus("");
        } catch (error) {
            assistantLoadingNode.remove();
            ns.setChatSendStatus(error.message || "Unable to regenerate that reply right now.", true);
            ns.notify("error", error.message || "Unable to regenerate that reply right now.");
        } finally {
            ns.setMessageSendingState(false);
            promptInput?.focus();
        }
    };

    ns.submitChatPrompt = async (options = {}) => {
        const conversationId = ns.getCurrentConversationId();
        if (!conversationId) {
            ns.notify("warning", "Open a conversation first.");
            return;
        }

        const query = String(options.queryOverride ?? promptInput?.value ?? "").trim();
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

        const explicitEditMessageId = String(options.editMessageId || "").trim();
        const pendingReplay = state.pendingReplay;
        const replayTargetMessageId = explicitEditMessageId || (pendingReplay?.mode === "edit" ? String(pendingReplay?.targetMessageId || "").trim() : "");
        const isEditingReplay = Boolean(replayTargetMessageId);
        const inlineTargetNode = options.inlineTargetNode instanceof HTMLElement ? options.inlineTargetNode : null;

        const optimisticUserNode = isEditingReplay ? null : ns.createUserMessageNode({ message_text: query });
        const assistantLoadingNode = ns.createAssistantLoadingNode(ns.getSelectedSourceDocumentLabels());
        if (optimisticUserNode) {
            ns.appendMessageNodes([optimisticUserNode, assistantLoadingNode]);
        } else if (inlineTargetNode) {
            inlineTargetNode.insertAdjacentElement("afterend", assistantLoadingNode);
            ns.updateScrollToBottomButton?.();
        } else {
            ns.appendMessageNodes([assistantLoadingNode]);
        }
        if (!options.queryOverride && promptInput) {
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
                    ...(isEditingReplay ? { edit_message_id: replayTargetMessageId } : {}),
                }),
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok) {
                throw new Error(payload.error || `Request failed with HTTP ${response.status}`);
            }

            if (isEditingReplay) {
                assistantLoadingNode.remove();
                optimisticUserNode?.remove();
            }
            if (isEditingReplay && Array.isArray(payload.conversation_messages)) {
                ns.renderConversationMessages(payload.conversation_messages);
            } else if (payload.messages?.assistant) {
                if (payload.messages?.user) {
                    optimisticUserNode.dataset.messageId = payload.messages.user.message_id || "";
                    optimisticUserNode.dataset.familyId = payload.messages.user.family_id || "";
                    optimisticUserNode.dataset.versionIndex = String(payload.messages.user.version_index || 1);
                    optimisticUserNode.dataset.versionCount = String(payload.messages.user.version_count || 1);
                }
                let assistantNode = null;
                try {
                    assistantNode = ns.createRetrievalInspectionNode(payload.messages.assistant);
                } catch (renderError) {
                    console.error("Failed to render retrieval inspection node:", renderError);
                    assistantNode = ns.createAssistantFallbackNode(payload.messages.assistant);
                }
                if (assistantNode) {
                    assistantLoadingNode.remove();
                    ns.appendMessageNodes([assistantNode]);
                }
            }
            state.pendingReplay = null;
            state.inlineMessageEdit = null;
            ns.setChatSendStatus("");
        } catch (error) {
            optimisticUserNode?.remove();
            assistantLoadingNode.remove();
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
        ns.hydrateInitialAssistantAnswers?.();
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

        chatMessageList.addEventListener("click", async (event) => {
            const editButton = event.target?.closest?.("[data-edit-message='true']");
            if (editButton) {
                const messageNode = editButton.closest("[data-message-id][data-message-role='user']");
                const messageId = String(messageNode?.dataset.messageId || "").trim();
                if (messageId) {
                    ns.beginMessageEdit(messageId);
                }
                return;
            }

            const inlineCancelButton = event.target?.closest?.("[data-inline-edit-cancel='true']");
            if (inlineCancelButton) {
                ns.closeInlineMessageEdit({ restoreOriginal: true });
                ns.setChatSendStatus("");
                return;
            }

            const inlineSendButton = event.target?.closest?.("[data-inline-edit-send='true']");
            if (inlineSendButton) {
                if (state.isSendingMessage) return;
                const messageId = String(inlineSendButton.dataset.messageId || "").trim();
                const messageNode = inlineSendButton.closest("[data-message-id][data-message-role='user']");
                const inlineInput = messageNode?.querySelector?.("[data-inline-edit-input='true']");
                const query = String(inlineInput?.value || "").trim();
                if (!messageId || !query) {
                    ns.setChatSendStatus("Enter a prompt before sending.", true);
                    return;
                }
                await ns.submitChatPrompt({
                    queryOverride: query,
                    editMessageId: messageId,
                    inlineTargetNode: messageNode,
                });
                return;
            }

            const regenerateButton = event.target?.closest?.("[data-regenerate-message='true']");
            if (regenerateButton) {
                const messageNode = regenerateButton.closest("[data-message-id][data-message-role='assistant']");
                const messageId = String(messageNode?.dataset.messageId || "").trim();
                if (messageId) {
                    ns.regenerateAssistantMessage(messageId);
                }
                return;
            }

            const citationTrigger = event.target?.closest?.("[data-open-citation='true']");
            if (citationTrigger) {
                const citation = ns.parseJsonDataAttribute(citationTrigger.dataset.citationPayload, {});
                ns.openCitationAnchor(citation);
                return;
            }

            const versionButton = event.target?.closest?.("[data-message-version-nav]");
            if (versionButton) {
                const familyId = String(versionButton.dataset.familyId || "").trim();
                const role = String(versionButton.dataset.role || "").trim().toLowerCase();
                const versionNumber = Number(versionButton.dataset.versionNumber || 0);
                const conversationId = ns.getCurrentConversationId();
                if (!familyId || !role || !versionNumber || !conversationId || state.isSendingMessage) {
                    return;
                }

                ns.setMessageSendingState(true);
                fetch(`/api/conversations/${encodeURIComponent(conversationId)}/message-versions/select`, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({
                        family_id: familyId,
                        role,
                        version_number: versionNumber,
                    }),
                })
                    .then((response) => response.json().catch(() => ({})).then((payload) => ({ response, payload })))
                    .then(({ response, payload }) => {
                        if (!response.ok) {
                            throw new Error(payload.error || `Request failed with HTTP ${response.status}`);
                        }
                        if (Array.isArray(payload.conversation_messages)) {
                            ns.renderConversationMessages(payload.conversation_messages);
                        }
                    })
                    .catch((error) => {
                        ns.notify("error", error.message || "Unable to switch message version right now.");
                    })
                    .finally(() => {
                        ns.setMessageSendingState(false);
                    });
            }
        });

        ns.updateScrollToBottomButton?.();
    };
}());
