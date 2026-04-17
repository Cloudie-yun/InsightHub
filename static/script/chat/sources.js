// ============================================================================
// CHAT PAGE SOURCES, DOCUMENT VIEWER, AND PENDING-PARSE SYNC
// ============================================================================
// This module covers source selection, document preview mode, DOM builders for
// uploaded documents, and polling for parser status changes.
(function initializeChatSourcesModule() {
    const ns = window.ChatPage;
    if (!ns) return;

    const {
        sendButton,
        sourcesDetailedList,
        sourcesIconList,
        sourcesCollapsedSummary,
        sourcesCollapsedIcon,
        sourcesCollapsedCount,
        sourcesSearchInput,
        sourcesSortSelect,
        sourcesSelectAllBtn,
        sourcesEmptyState,
        toolboxDocFrame,
        toolboxDocEmpty,
        toolboxDocTitle,
        toolboxDocOpen,
        toolboxDocBack,
    } = ns.elements;
    const state = ns.state;
    const constants = ns.constants;
    const sourceClickTimers = new WeakMap();

    ns.applySourceSelectButtonState = (button, isSelected) => {
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

    ns.getSourceSelectButtons = () => {
        if (!sourcesDetailedList) return [];
        return Array.from(sourcesDetailedList.querySelectorAll('[data-source-select-btn="true"]'));
    };

    ns.getSelectedSourceDocumentIds = () => (
        ns.getSourceSelectButtons()
            .filter((button) => button.dataset.selected === "true")
            .map((button) => String(button.closest("[data-doc-id]")?.dataset.docId || "").trim())
            .filter(Boolean)
    );

    ns.getVisibleSourceSelectButtons = () => (
        ns.getSourceSelectButtons().filter((button) => !button.closest(".hidden"))
    );

    ns.bindSourceSelectButton = (button) => {
        if (!button || button.dataset.boundSelectHandler === "true") return;
        button.dataset.boundSelectHandler = "true";
        const isSelected = button.dataset.selected === "true";
        ns.applySourceSelectButtonState(button, isSelected);
        button.addEventListener("click", (event) => {
            event.preventDefault();
            event.stopPropagation();
            const currentlySelected = button.dataset.selected === "true";
            ns.applySourceSelectButtonState(button, !currentlySelected);
            ns.updateSendButtonState();
            ns.updateSelectAllButtonState();
        });
    };

    ns.bindDocumentTrigger = (trigger) => {
        if (!trigger || trigger.dataset.boundDocHandler === "true") return;
        trigger.dataset.boundDocHandler = "true";
        trigger.addEventListener("dblclick", (event) => {
            if (event.target.closest("a")) return;
            if (event.target.closest('[data-source-select-btn="true"]')) return;
            const pendingClickTimer = sourceClickTimers.get(trigger);
            if (pendingClickTimer) {
                window.clearTimeout(pendingClickTimer);
                sourceClickTimers.delete(trigger);
            }
            const filePath = trigger.dataset.docFile;
            const fileTitle = trigger.dataset.docTitle || filePath;
            if (!filePath) return;
            ns.openToolboxDocument(filePath, fileTitle);
        });
    };

    ns.toggleSourceSelectionFromNode = (node) => {
        if (!node) return false;
        const selectButton = node.querySelector('[data-source-select-btn="true"]');
        if (!selectButton) return false;
        const currentlySelected = selectButton.dataset.selected === "true";
        ns.applySourceSelectButtonState(selectButton, !currentlySelected);
        ns.updateSendButtonState();
        ns.updateSelectAllButtonState();
        return true;
    };

    ns.bindSourceItemSelection = (node) => {
        if (!node || node.dataset.boundSourceSelection === "true") return;
        node.dataset.boundSourceSelection = "true";

        node.addEventListener("click", (event) => {
            if (event.target.closest("a")) return;
            if (event.target.closest('[data-source-select-btn="true"]')) return;

            const pendingTimer = sourceClickTimers.get(node);
            if (pendingTimer) {
                window.clearTimeout(pendingTimer);
            }

            const timerId = window.setTimeout(() => {
                sourceClickTimers.delete(node);
                ns.toggleSourceSelectionFromNode(node);
            }, 220);

            sourceClickTimers.set(node, timerId);
        });
    };

    ns.updateSelectAllButtonState = () => {
        if (!sourcesSelectAllBtn) return;
        const sourceButtons = ns.getVisibleSourceSelectButtons();
        sourcesSelectAllBtn.classList.toggle("hidden", sourceButtons.length === 0);
        const selectedCount = sourceButtons.filter((button) => button.dataset.selected === "true").length;
        const allSelected = sourceButtons.length > 0 && selectedCount === sourceButtons.length;
        sourcesSelectAllBtn.textContent = allSelected ? "Deselect All" : "Select All";
    };

    ns.updateSendButtonState = () => {
        if (!sendButton) return;
        const hasSelected = ns.getSelectedSourceDocumentIds().length > 0;
        const hasConversation = Boolean(ns.getCurrentConversationId());
        const shouldDisable = !hasSelected || !hasConversation || ns.state.isSendingMessage;
        sendButton.disabled = shouldDisable;
        sendButton.classList.toggle("opacity-50", shouldDisable);
        sendButton.classList.toggle("cursor-not-allowed", shouldDisable);
    };

    ns.initializeSourceSelectionButtons = () => {
        if (!sourcesDetailedList) return;
        const sourceButtons = ns.getSourceSelectButtons();
        sourceButtons.forEach(ns.bindSourceSelectButton);
        sourcesSelectAllBtn?.addEventListener("click", () => {
            const buttons = ns.getVisibleSourceSelectButtons();
            const allSelected = buttons.length > 0 && buttons.every((button) => button.dataset.selected === "true");
            const nextSelected = !allSelected;
            buttons.forEach((button) => {
                ns.applySourceSelectButtonState(button, nextSelected);
            });
            ns.updateSendButtonState();
            ns.updateSelectAllButtonState();
        });
        ns.updateSendButtonState();
        ns.updateSelectAllButtonState();
    };

    ns.showViewerFallback = (fileName) => {
        if (!toolboxDocEmpty) return;
        toolboxDocEmpty.replaceChildren();
        const text = document.createElement("p");
        text.className = "text-sm text-slate-500";
        text.textContent = `Preview is only available for PDF files. Open "${fileName}" in a new tab.`;
        toolboxDocEmpty.appendChild(text);
    };

    ns.openToolboxDocument = (filePath, displayName = "") => {
        if (!ns.elements.toolsDocumentContent || !filePath) return;

        if (state.isPanelCollapsed) {
            state.isPanelCollapsed = false;
            ns.applyPanelCollapseState();
            ns.snapPanelToSide(state.dockSide);
            ns.savePanelState();
        }

        if (state.widthBeforeDocumentMode === null) {
            state.widthBeforeDocumentMode = state.expandedPanelWidth;
        }

        state.expandedPanelWidth = ns.clamp(
            ns.getMaxAllowedPanelWidth(),
            constants.PANEL_MIN_WIDTH,
            constants.PANEL_MAX_WIDTH,
        );

        const safeDisplayName = (displayName || filePath).trim();
        const fileUrl = ns.getUploadUrl(filePath);
        const filePathLower = filePath.toLowerCase();
        const isPdf = filePathLower.endsWith(".pdf");
        const isDocx = filePathLower.endsWith(".docx");
        const previewUrl = isPdf ? fileUrl : (isDocx ? ns.getPreviewUrl(filePath) : null);

        state.isToolboxDocumentMode = true;
        ns.applyPanelCollapseState();
        ns.snapPanelToSide(state.dockSide);

        if (toolboxDocTitle) {
            toolboxDocTitle.textContent = safeDisplayName;
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
            ns.showViewerFallback(safeDisplayName);
        }
    };

    ns.closeToolboxDocument = () => {
        state.isToolboxDocumentMode = false;
        const fallbackWidth = 340;
        const restoredWidth = state.widthBeforeDocumentMode ?? fallbackWidth;
        state.expandedPanelWidth = ns.clamp(
            restoredWidth,
            constants.PANEL_MIN_WIDTH,
            ns.getMaxAllowedPanelWidth(),
        );
        state.widthBeforeDocumentMode = null;

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

        ns.applyPanelCollapseState();
        ns.snapPanelToSide(state.dockSide);
    };

    ns.initializeDocumentViewer = () => {
        document.querySelectorAll("[data-doc-file]").forEach(ns.bindDocumentTrigger);
        document.querySelectorAll("#sources-detailed-list [data-doc-id]").forEach(ns.bindSourceItemSelection);
        toolboxDocBack?.addEventListener("click", ns.closeToolboxDocument);
        sourcesCollapsedSummary?.addEventListener("click", () => {
            const firstVisibleNode = Array.from(
                sourcesDetailedList?.querySelectorAll("[data-doc-id], [data-temp-upload-id]") || [],
            ).find((node) => !node.classList.contains("hidden"));
            if (!firstVisibleNode) return;
            const filePath = firstVisibleNode.dataset.docFile;
            const fileTitle = firstVisibleNode.dataset.docTitle || filePath;
            if (!filePath) return;
            ns.openToolboxDocument(filePath, fileTitle);
        });
    };

    ns.normalizeSourceSortName = (value) => String(value || "").trim().toLocaleLowerCase();

    ns.getSourceTimestampValue = (value) => {
        if (typeof value === "number" && Number.isFinite(value)) {
            return value;
        }

        const normalizedValue = String(value || "").trim();
        if (!normalizedValue) {
            return 0;
        }

        const numericValue = Number(normalizedValue);
        if (Number.isFinite(numericValue)) {
            return numericValue;
        }

        const parsed = Date.parse(normalizedValue);
        return Number.isFinite(parsed) ? parsed : 0;
    };

    ns.getDocumentCreatedAtTimestamp = (documentPayload) => ns.getSourceTimestampValue(
        documentPayload?.uploaded_at_ts
        || documentPayload?.created_at_ts
        || documentPayload?.uploaded_at
        || documentPayload?.created_at,
    );

    ns.getSourceNodeTitle = (node) => String(node?.dataset.docTitle || node?.title || "").trim();

    ns.getSourceNodeTimestamp = (node) => ns.getSourceTimestampValue(
        node?.dataset.docCreatedAtTs
        || node?.dataset.docCreatedAt
        || node?.dataset.docUploadedAtTs
        || node?.dataset.docUploadedAt,
    );

    ns.compareSourceNodes = (leftNode, rightNode, sortMode) => {
        const leftTitle = ns.normalizeSourceSortName(ns.getSourceNodeTitle(leftNode));
        const rightTitle = ns.normalizeSourceSortName(ns.getSourceNodeTitle(rightNode));
        const leftTimestamp = ns.getSourceNodeTimestamp(leftNode);
        const rightTimestamp = ns.getSourceNodeTimestamp(rightNode);

        if (sortMode === "upload_asc") {
            return (leftTimestamp - rightTimestamp) || leftTitle.localeCompare(rightTitle);
        }
        if (sortMode === "name_asc") {
            return leftTitle.localeCompare(rightTitle) || (rightTimestamp - leftTimestamp);
        }
        if (sortMode === "name_desc") {
            return rightTitle.localeCompare(leftTitle) || (rightTimestamp - leftTimestamp);
        }
        return (rightTimestamp - leftTimestamp) || leftTitle.localeCompare(rightTitle);
    };

    ns.doesSourceNodeMatchSearch = (node, query) => {
        const normalizedQuery = String(query || "").trim().toLocaleLowerCase();
        if (!normalizedQuery) return true;
        return ns.normalizeSourceSortName(ns.getSourceNodeTitle(node)).includes(normalizedQuery);
    };

    ns.updateSourceEmptyState = () => {
        if (!sourcesEmptyState || !sourcesDetailedList) return;
        const hasVisibleDocument = Array.from(
            sourcesDetailedList.querySelectorAll("[data-doc-id], [data-temp-upload-id]"),
        ).some((node) => !node.classList.contains("hidden"));

        sourcesEmptyState.classList.toggle("hidden", hasVisibleDocument);
    };

    ns.updateCollapsedSourcesSummary = () => {
        if (!sourcesCollapsedSummary || !sourcesCollapsedIcon || !sourcesCollapsedCount || !sourcesDetailedList) return;

        const sourceNodes = Array.from(
            sourcesDetailedList.querySelectorAll("[data-doc-id], [data-temp-upload-id]"),
        );

        const totalDocuments = sourceNodes.length;
        sourcesCollapsedSummary.classList.toggle("hidden", totalDocuments === 0);

        if (!totalDocuments) {
            sourcesCollapsedSummary.title = "Documents";
            sourcesCollapsedCount.textContent = "0";
            sourcesCollapsedIcon.className = "h-10 w-10 rounded-lg bg-slate-100 text-slate-500 flex items-center justify-center";
            sourcesCollapsedIcon.innerHTML = '<i class="fa-regular fa-file-lines"></i>';
            return;
        }

        const firstNode = sourceNodes[0];
        const isProcessing = !!firstNode?.dataset.tempUploadId
            || String(firstNode?.dataset.parserStatus || "").toLowerCase() === constants.PENDING_PARSER_STATUS;
        const title = String(firstNode?.dataset.docTitle || "Documents").trim();

        sourcesCollapsedSummary.title = totalDocuments === 1 ? title : `${title} and ${totalDocuments - 1} more`;
        sourcesCollapsedCount.textContent = String(totalDocuments);
        sourcesCollapsedIcon.className = isProcessing
            ? "h-10 w-10 rounded-lg bg-brand-50 text-brand-700 border border-brand-200 flex items-center justify-center"
            : "h-10 w-10 rounded-lg bg-slate-100 text-slate-500 flex items-center justify-center";
        sourcesCollapsedIcon.innerHTML = isProcessing
            ? '<i class="fa-solid fa-spinner animate-spin"></i>'
            : '<i class="fa-regular fa-file-lines"></i>';
    };

    ns.applySourceFiltersAndSorting = () => {
        const detailedNodes = sourcesDetailedList
            ? Array.from(sourcesDetailedList.querySelectorAll("[data-doc-id], [data-temp-upload-id]"))
            : [];
        const sortMode = state.sourceSortMode || constants.DEFAULT_SOURCE_SORT_MODE;
        const query = state.sourceSearchQuery || "";

        detailedNodes
            .sort((leftNode, rightNode) => ns.compareSourceNodes(leftNode, rightNode, sortMode))
            .forEach((node) => {
                node.classList.toggle("hidden", !ns.doesSourceNodeMatchSearch(node, query));
                sourcesDetailedList?.appendChild(node);
            });

        ns.updateSourceEmptyState();
        ns.updateSelectAllButtonState();
        ns.updateCollapsedSourcesSummary();
    };

    ns.initializeSourceSearchAndSort = () => {
        if (sourcesSearchInput) {
            sourcesSearchInput.value = state.sourceSearchQuery;
            sourcesSearchInput.addEventListener("input", () => {
                state.sourceSearchQuery = sourcesSearchInput.value || "";
                ns.applySourceFiltersAndSorting();
            });
        }

        if (sourcesSortSelect) {
            sourcesSortSelect.value = state.sourceSortMode || constants.DEFAULT_SOURCE_SORT_MODE;
            sourcesSortSelect.addEventListener("change", () => {
                state.sourceSortMode = sourcesSortSelect.value || constants.DEFAULT_SOURCE_SORT_MODE;
                ns.applySourceFiltersAndSorting();
            });
        }

        ns.applySourceFiltersAndSorting();
    };

    ns.createSourceDetailedItem = (documentPayload) => {
        if (!sourcesDetailedList || !documentPayload?.upload_path || !documentPayload?.original_filename) return null;

        const parserStatus = String(documentPayload?.parser_status || "").toLowerCase() || constants.PENDING_PARSER_STATUS;
        const isProcessing = parserStatus === constants.PENDING_PARSER_STATUS;
        const createdAt = String(documentPayload?.uploaded_at || documentPayload?.created_at || "").trim();
        const article = document.createElement("article");
        article.className = "group rounded-xl bg-transparent hover:bg-brand-50/30 transition-colors cursor-pointer";
        article.dataset.docFile = documentPayload.upload_path;
        article.dataset.docTitle = documentPayload.original_filename;
        article.dataset.docId = documentPayload.document_id || "";
        article.dataset.docCreatedAt = createdAt;
        article.dataset.docCreatedAtTs = String(ns.getDocumentCreatedAtTimestamp(documentPayload));
        article.dataset.parserStatus = parserStatus;

        const row = document.createElement("div");
        row.className = "flex items-center gap-3 px-3 py-2.5";

        const iconWrap = document.createElement("span");
        iconWrap.className = "h-8 w-8 flex items-center justify-center rounded-lg bg-slate-100 text-slate-500";
        iconWrap.innerHTML = '<i class="fa-regular fa-file-lines"></i>';

        const nameWrap = document.createElement("div");
        nameWrap.className = "min-w-0 flex-1";

        const nameText = document.createElement("p");
        nameText.className = "truncate text-sm font-medium text-slate-700";
        nameText.title = documentPayload.original_filename;
        nameText.textContent = documentPayload.original_filename;
        nameWrap.appendChild(nameText);

        if (isProcessing) {
            const subText = document.createElement("p");
            subText.className = "mt-0.5 text-xs text-brand-700";
            subText.innerHTML = `<i class="fa-solid fa-spinner animate-spin mr-1"></i>${ns.escapeHtml(ns.getParserProgressMessage(documentPayload))}`;
            nameWrap.appendChild(subText);
        }

        const parseLink = document.createElement("a");
        parseLink.className = "h-7 px-2 inline-flex items-center rounded-lg border border-gray-200 text-xs text-slate-500 hover:text-brand-700 hover:border-brand-300 hover:bg-brand-50";
        parseLink.title = "Inspect parser results";
        parseLink.textContent = "Parse";
        if (documentPayload.document_id) {
            const currentConversationId = ns.getCurrentConversationId();
            const params = new URLSearchParams();
            if (currentConversationId) {
                params.set("conversation_id", currentConversationId);
            }
            const queryString = params.toString();
            parseLink.href = `/documents/${encodeURIComponent(documentPayload.document_id)}/parser-results${queryString ? `?${queryString}` : ""}`;
        } else {
            parseLink.href = "#";
            parseLink.classList.add("pointer-events-none", "opacity-60");
        }

        const selectButton = document.createElement("button");
        selectButton.type = "button";
        selectButton.className = "h-5 w-5 rounded border text-[10px] transition-colors flex items-center justify-center";
        selectButton.dataset.sourceSelectBtn = "true";
        selectButton.dataset.selected = "true";
        selectButton.setAttribute("aria-pressed", "true");
        selectButton.title = "Deselect Document";
        selectButton.innerHTML = '<i class="fa-solid fa-check"></i>';

        row.appendChild(iconWrap);
        row.appendChild(nameWrap);
        if (!isProcessing) {
            row.appendChild(parseLink);
        }
        if (isProcessing) {
            const statusBadge = document.createElement("span");
            statusBadge.className = "h-7 px-2 inline-flex items-center rounded-lg border border-brand-200 bg-brand-50 text-xs text-brand-700";
            statusBadge.textContent = "Processing";
            row.appendChild(statusBadge);
        }
        if (!isProcessing) {
            row.appendChild(selectButton);
        }

        article.appendChild(row);
        ns.bindDocumentTrigger(article);
        ns.bindSourceItemSelection(article);
        if (!isProcessing) {
            ns.bindSourceSelectButton(selectButton);
        }
        return article;
    };

    ns.createSourceIconItem = (_documentPayload) => null;

    ns.createProcessingSourceItems = (files) => {
        const ids = [];

        files.forEach((file, index) => {
            const tempId = `processing-${Date.now()}-${index}-${Math.random().toString(36).slice(2, 7)}`;
            ids.push(tempId);
            const etaLabel = ns.formatDurationLabel(ns.estimateProcessingSecondsForFile(file));
            const fileName = file?.name || "Uploading file";

            if (sourcesDetailedList) {
                const article = document.createElement("article");
                article.className = "rounded-xl bg-brand-50/40";
                article.dataset.tempUploadId = tempId;
                article.dataset.docTitle = fileName;
                article.dataset.docCreatedAtTs = String(Date.now() + index);

                const row = document.createElement("div");
                row.className = "flex items-center gap-3 px-3 py-2.5";

                const iconWrap = document.createElement("span");
                iconWrap.className = "h-8 w-8 flex items-center justify-center rounded-lg bg-white text-brand-700 border border-brand-100";
                iconWrap.innerHTML = '<i class="fa-solid fa-spinner animate-spin text-xs"></i>';

                const nameWrap = document.createElement("div");
                nameWrap.className = "min-w-0 flex-1";

                const nameText = document.createElement("p");
                nameText.className = "truncate text-sm font-medium text-slate-700";
                nameText.title = fileName;
                nameText.textContent = fileName;

                const subText = document.createElement("p");
                subText.className = "mt-0.5 text-xs text-brand-700";
                subText.textContent = `Processing... Estimated ${etaLabel}`;

                const badge = document.createElement("span");
                badge.className = "h-7 px-2 inline-flex items-center rounded-lg border border-brand-200 bg-white text-xs text-brand-700";
                badge.textContent = "Processing";

                nameWrap.appendChild(nameText);
                nameWrap.appendChild(subText);
                row.appendChild(iconWrap);
                row.appendChild(nameWrap);
                row.appendChild(badge);
                article.appendChild(row);
                sourcesDetailedList.appendChild(article);
            }

        });

        ns.applySourceFiltersAndSorting();

        return ids;
    };

    ns.removeProcessingSourceItems = (ids) => {
        ids.forEach((id) => {
            document.querySelectorAll(`[data-temp-upload-id="${id}"]`).forEach((node) => node.remove());
        });
    };

    ns.removeSourceItemsByDocId = (documentId) => {
        const safeId = String(documentId || "").trim();
        if (!safeId) return;
        document.querySelectorAll(`[data-doc-id="${safeId}"]`).forEach((node) => node.remove());
    };

    ns.getParserProgressSignature = (documentPayload) => {
        const parserProgress = documentPayload?.parser_progress;
        if (!parserProgress || typeof parserProgress !== "object") {
            return "";
        }

        return JSON.stringify({
            stage: parserProgress.stage || "",
            message: parserProgress.message || "",
            provider: parserProgress.provider || "",
            provider_state: parserProgress.provider_state || "",
            batch_id: parserProgress.batch_id || "",
            task_id: parserProgress.task_id || "",
            extracted_pages: parserProgress.extracted_pages ?? null,
            total_pages: parserProgress.total_pages ?? null,
            progress_percent: parserProgress.progress_percent ?? null,
            updated_at: parserProgress.updated_at || "",
        });
    };

    ns.getDocumentVisualStateSignature = (documentPayload) => {
        const parserStatus = String(documentPayload?.parser_status || "").trim().toLowerCase() || constants.PENDING_PARSER_STATUS;
        return JSON.stringify({
            parser_status: parserStatus,
            parser_progress: ns.getParserProgressSignature(documentPayload),
        });
    };

    ns.findPrimarySourceNodeByDocId = (documentId) => {
        const safeId = String(documentId || "").trim();
        if (!safeId) return null;
        return document.querySelector(`#sources-detailed-list [data-doc-id="${safeId}"], #sources-icon-list [data-doc-id="${safeId}"]`);
    };

    ns.readExistingSourceVisualStateSignature = (documentId) => {
        const existingNode = ns.findPrimarySourceNodeByDocId(documentId);
        return String(existingNode?.dataset.visualStateSignature || "");
    };

    ns.readExistingSourceVisualStateDirty = (documentId) => {
        const existingNode = ns.findPrimarySourceNodeByDocId(documentId);
        return existingNode?.dataset.visualStateDirty === "true";
    };

    ns.readExistingSourceSelectionState = (documentId) => {
        const safeId = String(documentId || "").trim();
        if (!safeId) return true;
        const existingButton = document.querySelector(
            `#sources-detailed-list [data-doc-id="${safeId}"] [data-source-select-btn="true"]`,
        );
        if (!existingButton) return true;
        return existingButton.dataset.selected === "true";
    };

    ns.applySourceVisualStateMetadata = (node, documentPayload, options = {}) => {
        const { forceDirty = false } = options;
        if (!node) return;

        if (forceDirty) {
            delete node.dataset.visualStateSignature;
            node.dataset.visualStateDirty = "true";
            node.dataset.parserProgressSignature = "";
            return;
        }

        node.dataset.visualStateSignature = ns.getDocumentVisualStateSignature(documentPayload);
        node.dataset.parserProgressSignature = ns.getParserProgressSignature(documentPayload);
        delete node.dataset.visualStateDirty;
    };

    ns.upsertSourceItems = (documentPayload, options = {}) => {
        const { forceDirty = false } = options;
        const documentId = String(documentPayload?.document_id || "").trim();
        const nextSignature = ns.getDocumentVisualStateSignature(documentPayload);
        const previousSignature = ns.readExistingSourceVisualStateSignature(documentId);
        const previousIsDirty = ns.readExistingSourceVisualStateDirty(documentId);
        const preservedSelectionState = ns.readExistingSourceSelectionState(documentId);

        if (!forceDirty && documentId && !previousIsDirty && previousSignature && previousSignature === nextSignature) {
            return false;
        }

        if (documentId) {
            ns.removeSourceItemsByDocId(documentId);
        }

        const detailedArticle = ns.createSourceDetailedItem(documentPayload);
        const iconButton = ns.createSourceIconItem(documentPayload);

        if (detailedArticle) {
            sourcesDetailedList.appendChild(detailedArticle);
            ns.applySourceVisualStateMetadata(detailedArticle, documentPayload, { forceDirty });
            const selectButton = detailedArticle.querySelector('[data-source-select-btn="true"]');
            if (selectButton) {
                ns.applySourceSelectButtonState(selectButton, preservedSelectionState);
            }
        }
        if (iconButton) {
            sourcesIconList.appendChild(iconButton);
            ns.applySourceVisualStateMetadata(iconButton, documentPayload, { forceDirty });
        }

        ns.applySourceFiltersAndSorting();

        return true;
    };

    ns.refreshToolboxSourcesSection = async () => {
        const conversationId = ns.getCurrentConversationId();
        if (!conversationId) return;

        const response = await fetch(`/api/conversations/${encodeURIComponent(conversationId)}/documents`);
        if (!response.ok) {
            throw new Error(`Failed to refresh toolbox sources: HTTP ${response.status}`);
        }

        const payload = await response.json();
        const documents = Array.isArray(payload.documents) ? payload.documents : [];
        const liveIds = new Set(
            documents
                .map((doc) => String(doc?.document_id || "").trim())
                .filter(Boolean),
        );

        // Remove stale items that no longer exist in the latest API response.
        let hasVisualChanges = false;
        document.querySelectorAll("[data-doc-id]").forEach((node) => {
            const id = String(node.dataset.docId || "").trim();
            if (id && !liveIds.has(id)) {
                node.remove();
                hasVisualChanges = true;
            }
        });

        // Upsert each current document without clearing the whole panel first.
        documents.forEach((doc) => {
            hasVisualChanges = ns.upsertSourceItems(doc) || hasVisualChanges;
        });

        if (hasVisualChanges) {
            ns.updateSelectAllButtonState();
            ns.updateSendButtonState();
            ns.applySourceFiltersAndSorting();
        }
    };

    ns.getPendingDocumentIds = () => {
        const ids = new Set();
        document.querySelectorAll("[data-doc-id][data-parser-status]").forEach((node) => {
            const parserStatus = String(node.dataset.parserStatus || "").toLowerCase();
            const documentId = String(node.dataset.docId || "").trim();
            if (documentId && parserStatus === constants.PENDING_PARSER_STATUS) {
                ids.add(documentId);
            }
        });
        return ids;
    };

    ns.stopPendingSourcePolling = () => {
        if (!state.pendingSourceStatusPollHandle) return;
        window.clearInterval(state.pendingSourceStatusPollHandle);
        state.pendingSourceStatusPollHandle = null;
    };

    ns.refreshPendingSourceStates = async () => {
        const conversationId = ns.getCurrentConversationId();
        if (!conversationId) {
            ns.stopPendingSourcePolling();
            return;
        }

        const pendingIds = ns.getPendingDocumentIds();
        if (!pendingIds.size) {
            ns.stopPendingSourcePolling();
            return;
        }

        try {
            await ns.refreshToolboxSourcesSection();
            if (!ns.getPendingDocumentIds().size) {
                ns.stopPendingSourcePolling();
            }
        } catch (_error) {
            // Keep polling on transient network/backend errors.
        }
    };

    ns.ensurePendingSourcePolling = () => {
        if (state.pendingSourceStatusPollHandle) return;
        state.pendingSourceStatusPollHandle = window.setInterval(
            ns.refreshPendingSourceStates,
            constants.PENDING_SOURCE_POLL_INTERVAL_MS,
        );
    };

    ns.initializePendingSourcePolling = () => {
        if (!ns.getCurrentConversationId()) return;
        if (!ns.getPendingDocumentIds().size) return;
        ns.ensurePendingSourcePolling();
        ns.refreshPendingSourceStates();
    };
}());
