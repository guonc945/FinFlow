import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { ChevronDown, Check } from 'lucide-react';
import './Select.css';

export interface SelectOption {
    value: string | number;
    label: string;
    disabled?: boolean;
}

export interface SelectOptionGroup {
    label: string;
    options: SelectOption[];
}

export interface SelectProps {
    value?: string | number;
    onChange?: (value: string) => void;
    options?: SelectOption[];
    groups?: SelectOptionGroup[];
    placeholder?: string;
    disabled?: boolean;
    className?: string;
    name?: string;
    style?: React.CSSProperties;
    dropdownMinWidth?: number;
}

function flattenOptions(groups: SelectOptionGroup[]): SelectOption[] {
    return groups.flatMap(g => g.options);
}

export default function Select({
    value,
    onChange,
    options,
    groups,
    placeholder = '请选择',
    disabled = false,
    className = '',
    name,
    style,
    dropdownMinWidth
}: SelectProps) {
    const [isOpen, setIsOpen] = useState(false);
    const [highlightedIndex, setHighlightedIndex] = useState(-1);
    const [dropdownStyle, setDropdownStyle] = useState<React.CSSProperties>({});
    const containerRef = useRef<HTMLDivElement>(null);
    const listRef = useRef<HTMLDivElement>(null);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const isGrouped = !!groups && groups.length > 0;
    const allOptions = useMemo(
        () => (isGrouped ? flattenOptions(groups!) : (options ?? [])),
        [groups, isGrouped, options]
    );
    const availableOptions = useMemo(() => {
        return allOptions.filter(opt => !opt.disabled);
    }, [allOptions]);

    const normalizedValue = value == null ? '' : String(value);
    const selectedOption = allOptions.find(opt => String(opt.value) === normalizedValue);
    const displayText = selectedOption?.label || (value ? value : placeholder);

    const updateDropdownPosition = useCallback(() => {
        if (!containerRef.current) {
            return;
        }

        const rect = containerRef.current.getBoundingClientRect();
        const width = dropdownMinWidth ? Math.max(rect.width, dropdownMinWidth) : rect.width;
        setDropdownStyle({
            position: 'fixed',
            top: rect.bottom + 4,
            left: rect.left,
            width,
            zIndex: 9999,
        });
    }, [dropdownMinWidth]);

    const handleToggle = useCallback(() => {
        if (!disabled) {
            setIsOpen(prev => {
                const nextIsOpen = !prev;
                if (nextIsOpen) {
                    const selectedIndex = availableOptions.findIndex(
                        (opt) => String(opt.value) === normalizedValue
                    );
                    setHighlightedIndex(selectedIndex);
                } else {
                    setHighlightedIndex(-1);
                }
                return nextIsOpen;
            });
        }
    }, [availableOptions, disabled, normalizedValue]);

    const handleSelect = useCallback((option: SelectOption) => {
        if (!option.disabled) {
            onChange?.(String(option.value));
            setIsOpen(false);
            setHighlightedIndex(-1);
        }
    }, [onChange]);

    const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
        if (!isOpen) {
            if (e.key === 'Enter' || e.key === ' ' || e.key === 'ArrowDown') {
                e.preventDefault();
                handleToggle();
            }
            return;
        }

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                setHighlightedIndex(prev => {
                    const next = prev + 1;
                    return next >= availableOptions.length ? 0 : next;
                });
                break;
            case 'ArrowUp':
                e.preventDefault();
                setHighlightedIndex(prev => {
                    const next = prev - 1;
                    return next < 0 ? availableOptions.length - 1 : next;
                });
                break;
            case 'Enter':
                e.preventDefault();
                if (highlightedIndex >= 0) {
                    if (availableOptions[highlightedIndex]) {
                        handleSelect(availableOptions[highlightedIndex]);
                    }
                }
                break;
            case 'Escape':
                e.preventDefault();
                setIsOpen(false);
                setHighlightedIndex(-1);
                break;
        }
    }, [isOpen, highlightedIndex, availableOptions, handleSelect, handleToggle]);

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const isOutsideContainer = containerRef.current && !containerRef.current.contains(event.target as Node);
            const isOutsideDropdown = dropdownRef.current && !dropdownRef.current.contains(event.target as Node);
            
            if (isOutsideContainer && isOutsideDropdown) {
                setIsOpen(false);
                setHighlightedIndex(-1);
            }
        };

        if (isOpen) {
            document.addEventListener('mousedown', handleClickOutside);
        }

        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, [isOpen]);

    useEffect(() => {
        if (!isOpen || highlightedIndex < 0 || !listRef.current) {
            return;
        }

        const highlightedElement = listRef.current.querySelector<HTMLElement>(
            `[data-option-index="${highlightedIndex}"]`
        );

        if (highlightedElement) {
            highlightedElement.scrollIntoView({ block: 'nearest' });
        }
    }, [highlightedIndex, isOpen]);

    useEffect(() => {
        if (!isOpen) {
            return;
        }

        updateDropdownPosition();
        window.addEventListener('resize', updateDropdownPosition);
        window.addEventListener('scroll', updateDropdownPosition, true);

        return () => {
            window.removeEventListener('resize', updateDropdownPosition);
            window.removeEventListener('scroll', updateDropdownPosition, true);
        };
    }, [isOpen, updateDropdownPosition]);

    const renderOption = (option: SelectOption) => {
        const isSelected = String(option.value) === normalizedValue;
        const availableIndex = availableOptions.indexOf(option);
        const isHighlighted = availableIndex === highlightedIndex;

        return (
            <div
                key={String(option.value)}
                data-option-index={availableIndex}
                className={`custom-select-option ${isSelected ? 'selected' : ''} ${isHighlighted ? 'highlighted' : ''} ${option.disabled ? 'disabled' : ''}`}
                onClick={() => handleSelect(option)}
                onMouseEnter={() => setHighlightedIndex(availableIndex)}
                role="option"
                aria-selected={isSelected}
            >
                <span className="custom-select-option-label">{option.label}</span>
                {isSelected && <Check size={14} className="custom-select-check" />}
            </div>
        );
    };

    const renderContent = () => {
        if (isGrouped) {
            return groups!.map((group, groupIdx) => (
                <div key={groupIdx} className="custom-select-group">
                    <div className="custom-select-group-label">{group.label}</div>
                    {group.options.map((option) => {
                        if (option.disabled) {
                            return null;
                        }
                        return renderOption(option);
                    })}
                </div>
            ));
        }

        return (options || []).map((option) => renderOption(option));
    };

    return (
        <div
            ref={containerRef}
            className={`custom-select ${isOpen ? 'open' : ''} ${disabled ? 'disabled' : ''} ${className}`}
            style={style}
            onKeyDown={handleKeyDown}
            tabIndex={disabled ? -1 : 0}
            role="combobox"
            aria-expanded={isOpen}
        >
            <div
                className="custom-select-trigger"
                onClick={handleToggle}
            >
                <span className={`custom-select-value ${!selectedOption && !value ? 'placeholder' : ''}`}>
                    {displayText}
                </span>
                <ChevronDown size={16} className="custom-select-arrow" />
            </div>

            {isOpen && createPortal(
                <div
                    className="custom-select-dropdown"
                    ref={dropdownRef}
                    style={dropdownStyle}
                >
                    <div ref={listRef}>
                        {renderContent()}
                    </div>
                </div>,
                document.body
            )}

            {name && (
                <input type="hidden" name={name} value={normalizedValue} />
            )}
        </div>
    );
}
