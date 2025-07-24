import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Nav } from 'react-bootstrap';
import { 
  FaChartLine, 
  FaListUl, 
  FaChartPie, 
  FaUserTie
} from 'react-icons/fa';
import './Sidebar.css';

const Sidebar = ({ isOpen }) => {
  const location = useLocation();
  
  const isActive = (path) => {
    return location.pathname === path ? 'active' : '';
  };

  return (
    <div className={`sidebar ${isOpen ? 'open' : ''}`}>
      <div className="sidebar-header">
        <h3>KRX 주식분석</h3>
      </div>
      <div className="sidebar-content">
        <Nav className="flex-column">
          <Nav.Link 
            as={Link} 
            to="/" 
            className={`sidebar-link ${isActive('/')}`}
          >
            <FaChartLine className="icon" />
            <span>대시보드</span>
          </Nav.Link>
          <Nav.Link 
            as={Link} 
            to="/stocks" 
            className={`sidebar-link ${isActive('/stocks')}`}
          >
            <FaListUl className="icon" />
            <span>종목 리스트</span>
          </Nav.Link>
          <Nav.Link 
            as={Link} 
            to="/investor-trends" 
            className={`sidebar-link ${isActive('/investor-trends')}`}
          >
            <FaChartPie className="icon" />
            <span>투자자 동향</span>
          </Nav.Link>
        </Nav>
      </div>
      <div className="sidebar-footer">
        <p>v0.1.0</p>
      </div>
    </div>
  );
};

export default Sidebar;
