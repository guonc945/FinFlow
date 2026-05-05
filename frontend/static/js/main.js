// 全局状态管理
const appState = {
    currentPage: 'dashboard',
    stats: null,
    incomeTrend: null,
    chargeItemsRanking: null,
    recentBills: null,
    projects: null,
    billsData: [],
    chargeItemsData: [],
    projectsData: []
};

// 页面路由管理
const router = {
    routes: {
        '/': 'dashboard',
        '/bills': 'bills',
        '/charge-items': 'charge-items',
        '/projects': 'projects',
        '/reports': 'reports',
        '/settings': 'settings'
    },
    
    navigateTo(path) {
        const page = this.routes[path] || 'dashboard';
        this.loadPage(page);
    },
    
    loadPage(pageName) {
        appState.currentPage = pageName;
        
        // 更新导航高亮
        document.querySelectorAll('.nav-item').forEach(item => {
            item.classList.remove('active');
            if (item.dataset.page === pageName) {
                item.classList.add('active');
            }
        });
        
        // 更新页面标题
        document.querySelector('.page-title').textContent = getPageTitle(pageName);
        
        // 加载对应页面内容
        switch(pageName) {
            case 'dashboard':
                loadDashboard();
                break;
            case 'bills':
                loadBillsPage();
                break;
            case 'charge-items':
                loadChargeItemsPage();
                break;
            case 'projects':
                loadProjectsPage();
                break;
            default:
                loadDashboard();
        }
    }
};

// 获取页面标题
function getPageTitle(pageName) {
    const titles = {
        'dashboard': '仪表盘',
        'bills': '账单管理',
        'charge-items': '收费项目',
        'projects': '项目管理',
        'reports': '报表分析',
        'settings': '系统设置'
    };
    return titles[pageName] || '仪表盘';
}

// 初始化应用
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

function initializeApp() {
    setupEventListeners();
    router.loadPage('dashboard');
}

function setupEventListeners() {
    const sidebar = document.querySelector('.sidebar');
    const menuToggle = document.querySelector('.menu-toggle');
    
    // 侧边栏菜单切换
    if (menuToggle && sidebar) {
        menuToggle.addEventListener('click', function() {
            sidebar.classList.toggle('open');
        });
    }
    
    // 点击外部关闭侧边栏
    document.addEventListener('click', function(e) {
        if (window.innerWidth <= 1024 && 
            sidebar && 
            !sidebar.contains(e.target) && 
            !menuToggle.contains(e.target)) {
            sidebar.classList.remove('open');
        }
    });
    
    // 导航项点击事件
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            const page = this.dataset.page;
            router.loadPage(page);
            
            if (window.innerWidth <= 1024) {
                sidebar.classList.remove('open');
            }
        });
    });
    
    // 全局搜索
    const globalSearch = document.getElementById('global-search');
    if (globalSearch) {
        globalSearch.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                performGlobalSearch(this.value);
            }
        });
    }
    
    // 通知按钮
    const notificationsBtn = document.getElementById('notifications-btn');
    if (notificationsBtn) {
        notificationsBtn.addEventListener('click', function() {
            showNotificationsPanel();
        });
    }
    
    // 设置按钮
    const settingsBtn = document.getElementById('settings-btn');
    if (settingsBtn) {
        settingsBtn.addEventListener('click', function() {
            showSettingsPanel();
        });
    }
    
    // 仪表盘特定事件监听器
    setupDashboardEventListeners();
}

function setupDashboardEventListeners() {
    // 收入趋势图表周期切换
    document.querySelectorAll('[data-period]').forEach(button => {
        button.addEventListener('click', function() {
            const period = this.dataset.period;
            loadIncomeTrend(period);
            
            // 更新按钮状态
            document.querySelectorAll('[data-period]').forEach(btn => {
                btn.classList.remove('btn-outline');
                if (btn !== this) {
                    btn.classList.add('btn-outline');
                }
            });
        });
    });
    
    // 查看全部按钮
    document.getElementById('view-all-bills')?.addEventListener('click', function() {
        router.loadPage('bills');
    });
    
    document.getElementById('view-all-charge-items')?.addEventListener('click', function() {
        router.loadPage('charge-items');
    });
    
    document.getElementById('view-all-projects')?.addEventListener('click', function() {
        router.loadPage('projects');
    });
}

// 仪表盘加载函数
async function loadDashboard() {
    try {
        // 显示加载状态
        showLoadingState();
        
        // 并行加载所有数据
        const [stats, incomeTrend, chargeItemsRanking, recentBills] = await Promise.all([
            api.getStats(),
            api.getIncomeTrend('month'),
            api.getChargeItemRanking(5),
            api.getRecentBills(5)
        ]);
        
        // 更新全局状态
        appState.stats = stats;
        appState.incomeTrend = incomeTrend;
        appState.chargeItemsRanking = chargeItemsRanking;
        appState.recentBills = recentBills;
        
        // 渲染数据
        renderStats(stats);
        renderIncomeChart(incomeTrend);
        renderChargeItemsRanking(chargeItemsRanking);
        renderRecentBills(recentBills);
        renderProjectStatus(); // 使用模拟数据直到有API
        
        // 移除加载状态
        removeLoadingState();
        
    } catch (error) {
        console.error('加载仪表盘数据失败:', error);
        showErrorMessage('加载数据失败，请稍后重试');
        removeLoadingState();
    }
}

// 显示加载状态
function showLoadingState() {
    document.querySelectorAll('.stat-card').forEach(card => {
        card.classList.add('loading');
    });
    
    document.querySelector('#income-chart .loading-placeholder')?.remove();
    document.querySelector('#charge-items-ranking .loading-data')?.closest('tr')?.remove();
    document.querySelector('#recent-bills .loading-data')?.closest('tr')?.remove();
    document.querySelector('#project-status-list .loading-placeholder')?.remove();
}

// 移除加载状态
function removeLoadingState() {
    document.querySelectorAll('.stat-card').forEach(card => {
        card.classList.remove('loading');
    });
}

// 渲染统计卡片
function renderStats(stats) {
    const statCards = [
        { selector: '.stat-value:nth-child(2)', value: stats.total_bills?.toLocaleString() || '0' },
        { selector: '.stat-value:nth-child(2)', value: formatCurrency(stats.total_income || 0), index: 1 },
        { selector: '.stat-value:nth-child(2)', value: stats.pending_bills?.toLocaleString() || '0', index: 2 },
        { selector: '.stat-value:nth-child(2)', value: stats.total_projects?.toLocaleString() || '0', index: 3 }
    ];
    
    const changeIndicators = [
        { selector: '.stat-change', value: '+12.5%', className: 'positive' },
        { selector: '.stat-change', value: '+8.3%', className: 'positive', index: 1 },
        { selector: '.stat-change', value: '-3.2%', className: 'negative', index: 2 },
        { selector: '.stat-change', value: '+2', className: 'positive', index: 3 }
    ];
    
    document.querySelectorAll('.stat-value').forEach((el, index) => {
        if (statCards[index]) {
            el.textContent = statCards[index].value;
        }
    });
    
    document.querySelectorAll('.stat-change').forEach((el, index) => {
        if (changeIndicators[index]) {
            el.textContent = changeIndicators[index].value;
            el.className = `stat-change ${changeIndicators[index].className}`;
        }
    });
}

// 渲染收入图表
function renderIncomeChart(data) {
    const chartContainer = document.getElementById('income-chart');
    if (!chartContainer) return;
    
    // 清空容器
    chartContainer.innerHTML = '';
    
    // 创建图表元素
    const chartDiv = document.createElement('div');
    chartDiv.className = 'income-chart';
    
    // 生成柱状图
    const maxValue = Math.max(...data.data) || 100;
    data.labels.forEach((label, index) => {
        const barContainer = document.createElement('div');
        barContainer.className = 'chart-bar-container';
        
        const percentage = (data.data[index] / maxValue) * 100;
        const height = Math.max(10, percentage); // 最小高度10%
        
        barContainer.innerHTML = `
            <div class="chart-bar" style="height: ${height}%;" title="${formatCurrency(data.data[index])}">
                <span class="chart-value">${formatCurrency(data.data[index]).replace('¥', '')}</span>
            </div>
            <div class="chart-label">${label}月</div>
        `;
        
        chartDiv.appendChild(barContainer);
    });
    
    chartContainer.appendChild(chartDiv);
}

// 渲染收费项目排行
function renderChargeItemsRanking(items) {
    const container = document.getElementById('charge-items-ranking');
    if (!container) return;
    
    container.innerHTML = '';
    
    items.forEach((item, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${index + 1}</td>
            <td>${item.item_name || '未知项目'}</td>
            <td>${formatCurrency(item.amount)}</td>
            <td>${item.percentage?.toFixed(1) || '0'}%</td>
        `;
        container.appendChild(row);
    });
}

// 渲染最近账单
function renderRecentBills(bills) {
    const container = document.getElementById('recent-bills');
    if (!container) return;
    
    container.innerHTML = '';
    
    bills.forEach(bill => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${bill.id || 'N/A'}</td>
            <td>${bill.charge_item_name || '未知项目'}</td>
            <td>${formatCurrency(bill.bill_amount)}</td>
            <td><span class="badge ${getStatusClass(bill.pay_status_str)}">${bill.pay_status_str || '未知'}</span></td>
            <td>${formatDate(bill.created_at) || 'N/A'}</td>
        `;
        container.appendChild(row);
    });
}

// 渲染项目状态
function renderProjectStatus() {
    const container = document.getElementById('project-status-list');
    if (!container) return;
    
    // 这里应该从API获取实际的项目数据，暂时使用模拟数据
    const mockProjects = [
        { name: '阳光花园', desc: '住宅小区', status: '运营中', statusClass: 'success' },
        { name: '商业广场', desc: '商业综合体', status: '运营中', statusClass: 'success' },
        { name: '办公大厦', desc: '办公楼宇', status: '维护中', statusClass: 'warning' },
        { name: '科技园区', desc: '产业园区', status: '建设中', statusClass: 'info' }
    ];
    
    container.innerHTML = '';
    
    mockProjects.forEach(project => {
        const item = document.createElement('div');
        item.className = 'project-status-item';
        item.innerHTML = `
            <div class="project-info">
                <div class="project-name">${project.name}</div>
                <div class="project-desc">${project.desc}</div>
            </div>
            <div class="project-status">
                <span class="badge ${project.statusClass}">${project.status}</span>
            </div>
        `;
        container.appendChild(item);
    });
}

// 辅助函数
function getStatusClass(status) {
    switch(status) {
        case '已缴':
            return 'success';
        case '待缴':
            return 'warning';
        default:
            return 'info';
    }
}

function formatCurrency(value) {
    return new Intl.NumberFormat('zh-CN', {
        style: 'currency',
        currency: 'CNY',
        minimumFractionDigits: 2
    }).format(value);
}

function formatDate(dateString) {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return new Intl.DateTimeFormat('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    }).format(date);
}

function showErrorMessage(message) {
    showToast(message, 'error');
}

function showToast(message, type = 'info') {
    // 移除现有的toast
    document.querySelectorAll('.toast').forEach(toast => toast.remove());
    
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    toast.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 12px 20px;
        background: ${type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6'};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        z-index: 10000;
        animation: slideIn 0.3s ease;
        font-weight: 500;
        min-width: 250px;
    `;
    
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// 其他页面加载函数
async function loadBillsPage() {
    const contentWrapper = document.querySelector('.content-wrapper');
    const template = document.getElementById('bills-page-template');
    
    if (contentWrapper && template) {
        contentWrapper.innerHTML = template.innerHTML;
        await loadBillsData();
        setupBillsPageEvents();
    }
}

async function loadChargeItemsPage() {
    const contentWrapper = document.querySelector('.content-wrapper');
    const template = document.getElementById('charge-items-page-template');
    
    if (contentWrapper && template) {
        contentWrapper.innerHTML = template.innerHTML;
        await loadChargeItemsData();
        setupChargeItemsPageEvents();
    }
}

async function loadProjectsPage() {
    const contentWrapper = document.querySelector('.content-wrapper');
    const template = document.getElementById('projects-page-template');
    
    if (contentWrapper && template) {
        contentWrapper.innerHTML = template.innerHTML;
        await loadProjectsData();
        setupProjectsPageEvents();
    }
}

// 数据加载函数
async function loadBillsData() {
    try {
        const bills = await api.getBills({ limit: 20 });
        appState.billsData = bills;
        renderBillsTable(bills);
    } catch (error) {
        console.error('加载账单数据失败:', error);
        showErrorMessage('加载账单数据失败');
    }
}

async function loadChargeItemsData() {
    try {
        const items = await api.getChargeItems({ limit: 20 });
        appState.chargeItemsData = items;
        renderChargeItemsTable(items);
    } catch (error) {
        console.error('加载收费项目数据失败:', error);
        showErrorMessage('加载收费项目数据失败');
    }
}

async function loadProjectsData() {
    try {
        const projects = await api.getProjects({ limit: 20 });
        appState.projectsData = projects;
        renderProjectsTable(projects);
    } catch (error) {
        console.error('加载项目数据失败:', error);
        showErrorMessage('加载项目数据失败');
    }
}

// 表格渲染函数
function renderBillsTable(bills) {
    const container = document.getElementById('bills-table-body');
    if (!container) return;
    
    container.innerHTML = '';
    
    bills.forEach(bill => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${bill.id || 'N/A'}</td>
            <td>${bill.charge_item_name || '未知项目'}</td>
            <td>${bill.full_house_name || '未知房屋'}</td>
            <td>${formatCurrency(bill.bill_amount)}</td>
            <td><span class="badge ${getStatusClass(bill.pay_status_str)}">${bill.pay_status_str || '未知'}</span></td>
            <td>${formatDate(bill.created_at) || 'N/A'}</td>
            <td>
                <button class="btn btn-sm btn-outline view-bill" data-id="${bill.id}">查看</button>
            </td>
        `;
        container.appendChild(row);
    });
    
    // 添加查看按钮事件
    document.querySelectorAll('.view-bill').forEach(button => {
        button.addEventListener('click', function() {
            const billId = this.dataset.id;
            viewBillDetails(billId);
        });
    });
}

function renderChargeItemsTable(items) {
    const container = document.getElementById('charge-items-table-body');
    if (!container) return;
    
    container.innerHTML = '';
    
    items.forEach((item, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${item.item_id || 'N/A'}</td>
            <td>${item.communityid || 'N/A'}</td>
            <td>${item.item_name || '未知项目'}</td>
            <td>${formatDate(item.created_at) || 'N/A'}</td>
            <td>
                <button class="btn btn-sm btn-outline edit-item" data-id="${item.item_id}">编辑</button>
                <button class="btn btn-sm btn-danger delete-item" data-id="${item.item_id}">删除</button>
            </td>
        `;
        container.appendChild(row);
    });
}

function renderProjectsTable(projects) {
    const container = document.getElementById('projects-table-body');
    if (!container) return;
    
    container.innerHTML = '';
    
    projects.forEach(project => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${project.proj_id || 'N/A'}</td>
            <td>${project.proj_name || '未知项目'}</td>
            <td>${formatDate(project.created_at) || 'N/A'}</td>
            <td>
                <button class="btn btn-sm btn-outline edit-project" data-id="${project.proj_id}">编辑</button>
                <button class="btn btn-sm btn-danger delete-project" data-id="${project.proj_id}">删除</button>
            </td>
        `;
        container.appendChild(row);
    });
}

// 页面事件设置函数
function setupBillsPageEvents() {
    document.getElementById('refresh-bills')?.addEventListener('click', loadBillsData);
    document.getElementById('export-bills')?.addEventListener('click', exportBills);
}

function setupChargeItemsPageEvents() {
    document.getElementById('refresh-charge-items')?.addEventListener('click', loadChargeItemsData);
    document.getElementById('add-charge-item')?.addEventListener('click', addChargeItem);
}

function setupProjectsPageEvents() {
    document.getElementById('refresh-projects')?.addEventListener('click', loadProjectsData);
    document.getElementById('add-project')?.addEventListener('click', addProject);
}

// 辅助操作函数
function performGlobalSearch(query) {
    if (!query.trim()) return;
    
    showToast(`搜索: ${query}`, 'info');
    // 实现全局搜索逻辑
}

function showNotificationsPanel() {
    showToast('通知中心', 'info');
    // 实现通知面板逻辑
}

function showSettingsPanel() {
    showToast('系统设置', 'info');
    // 实现设置面板逻辑
}

async function loadIncomeTrend(period = 'month') {
    try {
        const data = await api.getIncomeTrend(period);
        renderIncomeChart(data);
    } catch (error) {
        console.error('加载收入趋势失败:', error);
    }
}

function viewBillDetails(billId) {
    showToast(`查看账单详情: ${billId}`, 'info');
    // 实现账单详情查看逻辑
}

function exportBills() {
    showToast('导出账单功能', 'info');
    // 实现账单导出逻辑
}

function addChargeItem() {
    showToast('添加收费项目', 'info');
    // 实现添加收费项目逻辑
}

function addProject() {
    showToast('添加项目', 'info');
    // 实现添加项目逻辑
}

// 动画相关函数
function animateNumbers() {
    const statValues = document.querySelectorAll('.stat-value');
    statValues.forEach(stat => {
        const text = stat.textContent;
        const match = text.match(/[\d,]+/);
        if (match) {
            const finalValue = text.replace(/[^\d.-]/g, '');
            const prefix = text.match(/^[\D]*/)[0];
            animateValue(stat, 0, parseFloat(finalValue), 1000, prefix);
        }
    });
}

function animateValue(element, start, end, duration, prefix = '') {
    const startTime = performance.now();
    
    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const easeProgress = 1 - Math.pow(1 - progress, 3);
        const current = start + (end - start) * easeProgress;
        
        if (element.textContent.includes('¥')) {
            element.textContent = prefix + new Intl.NumberFormat('zh-CN', {
                style: 'currency',
                currency: 'CNY',
                minimumFractionDigits: end.toString().includes('.') ? 2 : 0
            }).format(current);
        } else {
            element.textContent = prefix + Math.floor(current).toLocaleString();
        }
        
        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }
    
    requestAnimationFrame(update);
}

// 响应式处理
window.addEventListener('resize', function() {
    const sidebar = document.querySelector('.sidebar');
    if (window.innerWidth > 1024 && sidebar) {
        sidebar.classList.remove('open');
    }
});