import SourceDictionaryManager from './SourceDictionaryManager';
import '../../styles/ResourceConsole.css';
import './DataCenter.css';

export default function DataCenterDictionariesPage() {
    return (
        <div className="page-container fade-in dictionary-page-wrapper">
            <SourceDictionaryManager />
        </div>
    );
}
