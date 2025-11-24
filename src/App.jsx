import  { useState } from 'react';
import MainLayout from './components/layout/MainLayout';
import DatasetExplorer from './pages/DatasetExplorer';
import ResultsView from './pages/ResultsView';
import ModelAnalytics from './pages/ModelAnalytics'; 
import ControlPanel from './pages/ControlPanel';    

const App = () => {
  const [activeTab, setActiveTab] = useState('dataset');

  const renderPage = () => {
    switch (activeTab) {
      case 'dataset': return <DatasetExplorer />;
      case 'results': return <ResultsView />;
      case 'analytics': return <ModelAnalytics />; 
      case 'control': return <ControlPanel />;    
      default: return <DatasetExplorer />;
    }
  };

  return (
    <MainLayout activeTab={activeTab} setActiveTab={setActiveTab}>
      {renderPage()}
    </MainLayout>
  );
};

export default App;