with open('src/pages/bills/index.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '// UI state\n    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);',
    '// UI state\n    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);\n    const [isConditionCollapsed, setIsConditionCollapsed] = useState(false);'
)

content = content.replace(
    '// UI state\r\n    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);',
    '// UI state\r\n    const [isFilterCollapsed, setIsFilterCollapsed] = useState(false);\r\n    const [isConditionCollapsed, setIsConditionCollapsed] = useState(false);'
)

content = content.replace(
    '{/* 次级工作流：条件过滤 */}\n                        <div className="action-row flex-wrap">',
    '{/* 次级工作流：条件过滤 */}\n                        <div className="flex items-center justify-between" style={{ marginBottom: isConditionCollapsed ? \'0\' : \'0.5rem\', padding: \'0 0.5rem\' }}>\n                            <span className="text-xs text-secondary font-medium">筛选条件</span>\n                            <button className="btn-text text-xs text-primary flex items-center gap-1" onClick={() => setIsConditionCollapsed(!isConditionCollapsed)}>\n                                {isConditionCollapsed ? \'展开筛选\' : \'收起筛选\'} {isConditionCollapsed ? <ChevronDown size={14} /> : <ChevronUp size={14} />}\n                            </button>\n                        </div>\n                        {!isConditionCollapsed && (\n                        <div className="action-row flex-wrap">'
)

content = content.replace(
    '{/* 次级工作流：条件过滤 */}\r\n                        <div className="action-row flex-wrap">',
    '{/* 次级工作流：条件过滤 */}\r\n                        <div className="flex items-center justify-between" style={{ marginBottom: isConditionCollapsed ? \'0\' : \'0.5rem\', padding: \'0 0.5rem\' }}>\r\n                            <span className="text-xs text-secondary font-medium">筛选条件</span>\r\n                            <button className="btn-text text-xs text-primary flex items-center gap-1" onClick={() => setIsConditionCollapsed(!isConditionCollapsed)}>\r\n                                {isConditionCollapsed ? \'展开筛选\' : \'收起筛选\'} {isConditionCollapsed ? <ChevronDown size={14} /> : <ChevronUp size={14} />}\r\n                            </button>\r\n                        </div>\r\n                        {!isConditionCollapsed && (\r\n                        <div className="action-row flex-wrap">'
)

with open('src/pages/bills/index.tsx', 'w', encoding='utf-8') as f:
    f.write(content)
