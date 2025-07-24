import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Container } from 'react-bootstrap';
import './App.css';

// 컴포넌트 import
import Sidebar from './components/Sidebar';
import Header from './components/Header';
import Dashboard from './pages/Dashboard';
import StockList from './pages/StockList';
import StockDetail from './pages/StockDetail';
import InvestorTrends from './pages/InvestorTrends';

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const toggleSidebar = () => {
    setSidebarOpen(!sidebarOpen);
  };

  return (
    <Router>
      <div className="app-container">
        <Sidebar isOpen={sidebarOpen} />
        <div className={`main-content ${sidebarOpen ? '' : 'expanded'}`}>
          <Header toggleSidebar={toggleSidebar} />
          <Container fluid className="content-container">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/stocks" element={<StockList />} />
              <Route path="/stock/:stockCode" element={<StockDetail />} />
              <Route path="/investor-trends" element={<InvestorTrends />} />
            </Routes>
          </Container>
        </div>
      </div>
    </Router>
  );
}

export default App;
