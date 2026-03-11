import { useState, useRef } from 'react';
import { AlertCircle, Search } from 'lucide-react';
import type { AccountingSubject } from '../../types';
import SubjectPickerModal from './SubjectPickerModal';
import './AccountSelector.css';

interface AccountSelectorProps {
    value: string;
    onChange: (val: string) => void;
    onFocus?: () => void;
    subjects: AccountingSubject[];
    placeholder?: string;
}

const AccountSelector = ({ value, onChange, onFocus, subjects, placeholder }: AccountSelectorProps) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const wrapperRef = useRef<HTMLDivElement>(null);

    // Validate Check logic
    const validateInput = (val: string) => {
        if (!val) {
            setError(null);
            return;
        }

        if (val.includes('{') || val.includes('}')) {
            setError(null);
            return;
        }

        const exactMatch = subjects.find(s => s.number === val);
        if (!exactMatch) {
            setError('科目不存在');
        } else if (!exactMatch.is_leaf) {
            setError('只能选择末级科目');
        } else {
            setError(null);
        }
    };

    const handleSelect = (subject: AccountingSubject) => {
        onChange(subject.number);
        setIsModalOpen(false);
        setError(null);
    };

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        const val = e.target.value;
        onChange(val);
        if (error) setError(null);
    };

    const [isFocused, setIsFocused] = useState(false);
    const matchedSubject = subjects.find(s => s.number === value);

    const handleFocus = () => {
        setIsFocused(true);
        onFocus?.();
    };

    const handleInternalBlur = () => {
        setIsFocused(false);
        validateInput(value);
    };

    return (
        <>
            <div className="account-selector-wrapper" ref={wrapperRef}>
                <div className={`input-container ${error ? 'has-error' : ''} ${isFocused ? 'is-focused' : ''} ${matchedSubject ? 'has-matched' : ''}`}>
                    <input
                        type="text"
                        className="account-input"
                        value={value}
                        onChange={handleInputChange}
                        onBlur={handleInternalBlur}
                        onFocus={handleFocus}
                        placeholder={placeholder || "输入或选择科目"}
                    />

                    {/* Display Name Badge when not focused and matched */}
                    {matchedSubject && !isFocused && (
                        <div className="selection-badge" onClick={() => setIsModalOpen(true)}>
                            <span className="badge-code">{matchedSubject.number}</span>
                            <span className="badge-name">{matchedSubject.fullname || matchedSubject.name}</span>
                        </div>
                    )}

                    {!error && (
                        <button
                            type="button"
                            onClick={() => setIsModalOpen(true)}
                            className="selector-trigger-btn"
                            tabIndex={-1}
                            title="选择科目"
                        >
                            <Search size={14} />
                        </button>
                    )}
                    {error && (
                        <span title={error} className="error-icon">
                            <AlertCircle size={14} />
                        </span>
                    )}
                </div>
                {matchedSubject && isFocused && (
                    <div className="matched-subject-path-floating" title={matchedSubject.fullname}>
                        {matchedSubject.fullname}
                    </div>
                )}
            </div>

            <SubjectPickerModal
                isOpen={isModalOpen}
                onClose={() => setIsModalOpen(false)}
                onSelect={handleSelect}
                subjects={subjects}
            />
        </>
    );
};

export default AccountSelector;
