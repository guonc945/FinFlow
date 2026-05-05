document.addEventListener('DOMContentLoaded', function () {
    const sidebar = document.querySelector('.sidebar');
    const menuToggle = document.querySelector('.menu-toggle');
    const searchInput = document.querySelector('.search-box input');
    const tableRows = document.querySelectorAll('.table tbody tr');

    if (menuToggle && sidebar) {
        menuToggle.addEventListener('click', function () {
            sidebar.classList.toggle('open');
        });
    }

    document.addEventListener('click', function (e) {
        if (window.innerWidth <= 1024 &&
            sidebar &&
            !sidebar.contains(e.target) &&
            !menuToggle.contains(e.target)) {
            sidebar.classList.remove('open');
        }
    });

    if (searchInput) {
        searchInput.addEventListener('input', function (e) {
            const query = e.target.value.trim().toLowerCase();
            filterTable(query);
        });

        searchInput.addEventListener('keypress', function (e) {
            if (e.key === 'Enter') {
                const query = e.target.value.trim();
                if (query) {
                    searchBills(query);
                }
            }
        });
    }

    const filterSelects = document.querySelectorAll('.filter-group select');
    filterSelects.forEach(select => {
        select.addEventListener('change', function () {
            applyFilters();
        });
    });

    const dateInputs = document.querySelectorAll('.filter-group input[type="date"]');
    dateInputs.forEach(input => {
        input.addEventListener('change', function () {
            applyFilters();
        });
    });

    tableRows.forEach(row => {
        row.addEventListener('click', function () {
            tableRows.forEach(r => r.style.backgroundColor = '');
            this.style.backgroundColor = 'rgba(37, 99, 235, 0.05)';
        });
    });

    const pageButtons = document.querySelectorAll('.page-btn:not(:disabled)');
    pageButtons.forEach(btn => {
        btn.addEventListener('click', function () {
            if (!this.classList.contains('active') && !this.disabled) {
                changePage(this.textContent);
            }
        });
    });

    const viewButtons = document.querySelectorAll('.table .btn-sm');
    viewButtons.forEach(btn => {
        btn.addEventListener('click', function (e) {
            e.stopPropagation();
            const row = this.closest('tr');
            const billId = row.querySelector('.bill-id').textContent;
            showBillDetail(billId);
        });
    });

    const exportButton = document.querySelector('.card-actions .btn-primary');
    if (exportButton) {
        exportButton.addEventListener('click', function () {
            exportBills();
        });
    }

    initRowHighlighting();
});

function filterTable(query) {
    const tableRows = document.querySelectorAll('.table tbody tr');
    tableRows.forEach(row => {
        const text = row.textContent.toLowerCase();
        const match = text.includes(query);
        row.style.display = match ? '' : 'none';
    });
}

function applyFilters() {
    const statusFilter = document.querySelector('.filter-group select[value^="paid"], .filter-group select[value^="unpaid"], .filter-group select[value^="partial"]')?.value;
    const itemFilter = document.querySelector('.filter-group select[value^="1"], .filter-group select[value^="2"], .filter-group select[value^="3"], .filter-group select[value^="4"]')?.value;
    const dateInputs = document.querySelectorAll('.filter-group input[type="date"]');
    const startDate = dateInputs[0]?.value;
    const endDate = dateInputs[1]?.value;

    const tableRows = document.querySelectorAll('.table tbody tr');
    tableRows.forEach(row => {
        const statusBadge = row.querySelector('.badge');
        const status = statusBadge?.textContent;
        const dateCell = row.querySelectorAll('td')[4]?.textContent;

        let visible = true;

        if (statusFilter && status) {
            if (statusFilter === 'paid' && status !== '已缴') visible = false;
            if (statusFilter === 'unpaid' && status !== '待缴') visible = false;
            if (statusFilter === 'partial' && status !== '部分缴纳') visible = false;
        }

        if (itemFilter) {
            const itemCell = row.querySelectorAll('td')[2]?.textContent;
            const itemMap = {
                '1': '住宅物业服务费',
                '2': '车位服务费',
                '3': '电费',
                '4': '装修垃圾清运费'
            };
            if (itemMap[itemFilter] && !itemCell.includes(itemMap[itemFilter])) {
                visible = false;
            }
        }

        row.style.display = visible ? '' : 'none';
    });
}

function changePage(page) {
    console.log('切换到页面:', page);
    const pageButtons = document.querySelectorAll('.page-btn');
    pageButtons.forEach(btn => btn.classList.remove('active'));

    const targetBtn = Array.from(pageButtons).find(btn => btn.textContent === page);
    if (targetBtn) {
        targetBtn.classList.add('active');
    }

    showToast(`已切换到第 ${page} 页`, 'info');
}

function showBillDetail(billId) {
    console.log('查看账单详情:', billId);
    showToast(`正在加载账单详情: ${billId}`, 'info');
}

function searchBills(query) {
    console.log('搜索账单:', query);
    showToast(`正在搜索: ${query}`, 'info');
}

function exportBills() {
    console.log('导出账单数据');
    showToast('正在导出账单数据...', 'info');

    setTimeout(() => {
        showToast('账单数据导出成功', 'success');
    }, 1500);
}

function initRowHighlighting() {
    const tableRows = document.querySelectorAll('.table tbody tr');
    tableRows.forEach(row => {
        row.addEventListener('mouseenter', function () {
            if (!this.style.display || this.style.display !== 'none') {
                this.style.backgroundColor = 'rgba(37, 99, 235, 0.03)';
            }
        });
        row.addEventListener('mouseleave', function () {
            if (!this.style.backgroundColor || this.style.backgroundColor === 'rgba(37, 99, 235, 0.03)') {
                this.style.backgroundColor = '';
            }
        });
    });
}

function updateRecordCount(count) {
    const recordCountEl = document.querySelector('.record-count');
    if (recordCountEl) {
        recordCountEl.textContent = `共 ${count.toLocaleString()} 条记录`;
    }
}

// ===================== Community/Project Selection & Sync =====================

// State for community selection
const communityState = {
    projects: [],
    selected: new Map(), // Map<id, name>
    isDropdownOpen: false
};

/**
 * Load projects from API and populate the community select dropdown
 */
async function loadProjects() {
    try {
        const response = await fetch('/api/projects');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        communityState.projects = await response.json();
        renderCommunityList();
        console.log(`Loaded ${communityState.projects.length} projects for community selection.`);
    } catch (error) {
        console.error('Failed to load projects:', error);
        showToast('加载园区列表失败', 'error');
    }
}

/**
 * Render the community list in the dropdown
 */
function renderCommunityList(filter = '') {
    const listContainer = document.getElementById('communitySelectList');
    if (!listContainer) return;

    const filterLower = filter.toLowerCase();
    const filtered = communityState.projects.filter(p =>
        p.proj_name.toLowerCase().includes(filterLower) ||
        String(p.proj_id).includes(filterLower)
    );

    listContainer.innerHTML = filtered.map(project => {
        const isSelected = communityState.selected.has(String(project.proj_id));
        return `
            <div class="community-select-item ${isSelected ? 'selected' : ''}" 
                 data-id="${project.proj_id}" data-name="${project.proj_name}">
                <input type="checkbox" ${isSelected ? 'checked' : ''}>
                <span class="community-select-item-text">${project.proj_name}</span>
                <span class="community-select-item-id">${project.proj_id}</span>
            </div>
        `;
    }).join('');

    // Add click handlers
    listContainer.querySelectorAll('.community-select-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            const id = item.dataset.id;
            const name = item.dataset.name;
            toggleCommunitySelection(id, name);
        });
    });
}

/**
 * Toggle selection of a community
 */
function toggleCommunitySelection(id, name) {
    if (communityState.selected.has(id)) {
        communityState.selected.delete(id);
    } else {
        communityState.selected.set(id, name);
    }
    updateSelectionDisplay();
    renderCommunityList(document.getElementById('communitySearchInput')?.value || '');
}

/**
 * Update the trigger text and tags display
 */
function updateSelectionDisplay() {
    const triggerText = document.querySelector('.community-select-text');
    const tagsContainer = document.getElementById('selectedCommunities');

    const count = communityState.selected.size;

    if (triggerText) {
        if (count === 0) {
            triggerText.textContent = '请选择园区...';
            triggerText.classList.add('placeholder');
        } else {
            triggerText.textContent = `已选择 ${count} 个园区`;
            triggerText.classList.remove('placeholder');
        }
    }

    if (tagsContainer) {
        tagsContainer.innerHTML = Array.from(communityState.selected.entries()).map(([id, name]) => `
            <span class="selected-tag" data-id="${id}">
                ${name}
                <span class="selected-tag-remove" data-id="${id}">×</span>
            </span>
        `).join('');

        // Add remove handlers
        tagsContainer.querySelectorAll('.selected-tag-remove').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = btn.dataset.id;
                communityState.selected.delete(id);
                updateSelectionDisplay();
                renderCommunityList(document.getElementById('communitySearchInput')?.value || '');
            });
        });
    }
}

/**
 * Toggle dropdown visibility
 */
function toggleDropdown() {
    const dropdown = document.getElementById('communitySelectDropdown');
    const trigger = document.getElementById('communitySelectTrigger');

    communityState.isDropdownOpen = !communityState.isDropdownOpen;

    if (communityState.isDropdownOpen) {
        dropdown?.classList.add('show');
        trigger?.classList.add('active');
    } else {
        dropdown?.classList.remove('show');
        trigger?.classList.remove('active');
    }
}

/**
 * Select all communities
 */
function selectAllCommunities() {
    communityState.projects.forEach(p => {
        communityState.selected.set(String(p.proj_id), p.proj_name);
    });
    updateSelectionDisplay();
    renderCommunityList(document.getElementById('communitySearchInput')?.value || '');
}

/**
 * Clear all selections
 */
function clearAllSelections() {
    communityState.selected.clear();
    updateSelectionDisplay();
    renderCommunityList(document.getElementById('communitySearchInput')?.value || '');
}

/**
 * Get selected community IDs
 */
function getSelectedCommunityIds() {
    return Array.from(communityState.selected.keys()).map(id => parseInt(id, 10));
}

// ===================== Sync Progress Modal =====================

const syncProgressState = {
    isOpen: false,
    total: 0,
    current: 0,
    logs: []
};

function showSyncProgressModal(total) {
    syncProgressState.total = total;
    syncProgressState.current = 0;
    syncProgressState.logs = [];

    const overlay = document.getElementById('syncProgressOverlay');
    const progressBar = document.getElementById('syncProgressBar');
    const progressCurrent = document.getElementById('syncProgressCurrent');
    const progressPercent = document.getElementById('syncProgressPercent');
    const progressLog = document.getElementById('syncProgressLog');
    const progressIcon = document.getElementById('syncProgressIcon');
    const progressTitle = document.getElementById('syncProgressTitle');
    const progressSubtitle = document.getElementById('syncProgressSubtitle');
    const closeBtn = document.getElementById('syncProgressCloseBtn');

    // Reset UI
    progressBar.style.width = '0%';
    progressCurrent.textContent = `0 / ${total} 园区`;
    progressPercent.textContent = '0%';
    progressLog.innerHTML = '';
    progressIcon.textContent = '⏳';
    progressIcon.classList.remove('complete');
    progressTitle.textContent = '正在同步数据';
    progressSubtitle.textContent = '正在连接服务器...';
    closeBtn.style.display = 'none';

    overlay?.classList.add('show');
    syncProgressState.isOpen = true;
}

function updateSyncProgress(current, message, isSuccess = true) {
    syncProgressState.current = current;
    syncProgressState.logs.push({ message, isSuccess, time: new Date().toLocaleTimeString() });

    const percent = Math.round((current / syncProgressState.total) * 100);

    const progressBar = document.getElementById('syncProgressBar');
    const progressCurrent = document.getElementById('syncProgressCurrent');
    const progressPercent = document.getElementById('syncProgressPercent');
    const progressLog = document.getElementById('syncProgressLog');
    const progressSubtitle = document.getElementById('syncProgressSubtitle');

    progressBar.style.width = `${percent}%`;
    progressCurrent.textContent = `${current} / ${syncProgressState.total} 园区`;
    progressPercent.textContent = `${percent}%`;
    progressSubtitle.textContent = message;

    // Add log entry
    const logItem = document.createElement('div');
    logItem.className = `sync-progress-log-item ${isSuccess ? 'success' : 'error'}`;
    logItem.textContent = `[${syncProgressState.logs[syncProgressState.logs.length - 1].time}] ${message}`;
    progressLog.appendChild(logItem);
    progressLog.scrollTop = progressLog.scrollHeight;
}

function completeSyncProgress(success = true) {
    const progressIcon = document.getElementById('syncProgressIcon');
    const progressTitle = document.getElementById('syncProgressTitle');
    const progressSubtitle = document.getElementById('syncProgressSubtitle');
    const closeBtn = document.getElementById('syncProgressCloseBtn');

    progressIcon.textContent = success ? '✅' : '❌';
    progressIcon.classList.add('complete');
    progressTitle.textContent = success ? '同步完成' : '同步失败';
    progressSubtitle.textContent = success
        ? `成功同步 ${syncProgressState.total} 个园区的数据`
        : '部分园区同步失败，请查看日志';
    closeBtn.style.display = 'block';
}

function hideSyncProgressModal() {
    const overlay = document.getElementById('syncProgressOverlay');
    overlay?.classList.remove('show');
    syncProgressState.isOpen = false;
}

/**
 * Sync bills data for selected communities with progress display
 */
async function syncBillsData() {
    const communityIds = getSelectedCommunityIds();

    if (communityIds.length === 0) {
        showToast('请至少选择一个园区', 'warning');
        return;
    }

    const syncBtn = document.getElementById('syncDataBtn');
    if (syncBtn) {
        syncBtn.disabled = true;
    }

    // Show progress modal
    showSyncProgressModal(communityIds.length);

    try {
        // Send sync request to backend
        const response = await fetch('/api/bills/sync', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ community_ids: communityIds }),
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        console.log('Sync started:', result);

        // Simulate progress for each community (backend runs in background)
        // In a real implementation, you would poll a status endpoint
        for (let i = 0; i < communityIds.length; i++) {
            await new Promise(resolve => setTimeout(resolve, 1500 + Math.random() * 1000));
            const communityId = communityIds[i];
            const communityName = communityState.selected.get(String(communityId)) || `园区 ${communityId}`;
            updateSyncProgress(i + 1, `正在同步: ${communityName}`, true);
        }

        completeSyncProgress(true);
        showToast(`同步完成：${communityIds.length} 个园区`, 'success');
    } catch (error) {
        console.error('Failed to sync bills:', error);
        completeSyncProgress(false);
        showToast('同步请求失败', 'error');
    } finally {
        if (syncBtn) {
            syncBtn.disabled = false;
        }
    }
}

// ===================== Initialize on page load =====================
document.addEventListener('DOMContentLoaded', function () {
    // Load projects for community selector
    loadProjects();

    // Dropdown trigger handler
    const trigger = document.getElementById('communitySelectTrigger');
    if (trigger) {
        trigger.addEventListener('click', toggleDropdown);
    }

    // Close dropdown when clicking outside
    document.addEventListener('click', (e) => {
        const wrapper = document.getElementById('communitySelectWrapper');
        if (wrapper && !wrapper.contains(e.target) && communityState.isDropdownOpen) {
            toggleDropdown();
        }
    });

    // Search input handler
    const searchInput = document.getElementById('communitySearchInput');
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            renderCommunityList(e.target.value);
        });
        // Prevent dropdown from closing when clicking search
        searchInput.addEventListener('click', (e) => e.stopPropagation());
    }

    // Select all button
    const selectAllBtn = document.getElementById('selectAllBtn');
    if (selectAllBtn) {
        selectAllBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            selectAllCommunities();
        });
    }

    // Clear all button
    const clearAllBtn = document.getElementById('clearAllBtn');
    if (clearAllBtn) {
        clearAllBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            clearAllSelections();
        });
    }

    // Sync button
    const syncBtn = document.getElementById('syncDataBtn');
    if (syncBtn) {
        syncBtn.addEventListener('click', syncBillsData);
    }

    // Progress modal close button
    const closeBtn = document.getElementById('syncProgressCloseBtn');
    if (closeBtn) {
        closeBtn.addEventListener('click', hideSyncProgressModal);
    }
});