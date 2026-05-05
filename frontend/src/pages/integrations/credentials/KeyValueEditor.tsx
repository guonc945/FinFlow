import { useState, useEffect } from 'react';
import { Trash2, Plus, Hash } from 'lucide-react';
import './CredentialTabs.css';

interface KeyValue {
    key: string;
    value: string;
}

interface KeyValueEditorProps {
    jsonString?: string;
    onChange: (json: string) => void;
    onOpenPicker: (field: string, index: number) => void;
    title: string;
}

const KeyValueEditor = ({ jsonString, onChange, onOpenPicker, title }: KeyValueEditorProps) => {
    const [pairs, setPairs] = useState<KeyValue[]>([]);

    useEffect(() => {
        try {
            const data = jsonString ? JSON.parse(jsonString) : {};
            const initialPairs = Object.entries(data).map(([key, value]) => ({
                key,
                value: String(value)
            }));
            if (initialPairs.length === 0) initialPairs.push({ key: '', value: '' });
            setPairs(initialPairs);
        } catch (_error) {
            setPairs([{ key: '', value: '' }]);
        }
    }, [jsonString]);

    const updatePair = (index: number, field: 'key' | 'value', value: string) => {
        const newPairs = [...pairs];
        newPairs[index][field] = value;
        setPairs(newPairs);
        syncToJson(newPairs);
    };

    const addPair = () => {
        setPairs([...pairs, { key: '', value: '' }]);
    };

    const removePair = (index: number) => {
        const newPairs = pairs.filter((_, i) => i !== index);
        setPairs(newPairs);
        syncToJson(newPairs);
    };

    const syncToJson = (currentPairs: KeyValue[]) => {
        const obj: Record<string, string> = {};
        currentPairs.forEach(p => {
            if (p.key) obj[p.key] = p.value;
        });
        onChange(JSON.stringify(obj));
    };

    return (
        <div className="kv-editor-container">
            <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex justify-between items-center">
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{title}</label>
                <button
                    onClick={addPair}
                    className="flex items-center gap-1.5 text-blue-600 hover:text-blue-700 font-bold text-[10px] uppercase transition-colors"
                >
                    <Plus size={12} /> Add Row
                </button>
            </div>
            {pairs.map((pair, index) => (
                <div key={index} className="kv-row">
                    <input
                        className="kv-input"
                        placeholder="Key"
                        value={pair.key}
                        onChange={(e) => updatePair(index, 'key', e.target.value)}
                    />
                    <div className="flex-1 relative">
                        <input
                            className="kv-input w-full pr-8"
                            placeholder="Value"
                            value={pair.value}
                            onChange={(e) => updatePair(index, 'value', e.target.value)}
                            id={`kv-input-${title}-${index}`}
                        />
                        <button
                            onClick={() => onOpenPicker(`kv-input-${title}-${index}`, index)}
                            className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-300 hover:text-blue-500"
                        >
                            <Hash size={12} />
                        </button>
                    </div>
                    <button onClick={() => removePair(index)} className="kv-remove-btn">
                        <Trash2 size={14} />
                    </button>
                </div>
            ))}
        </div>

    );
};

export default KeyValueEditor;
