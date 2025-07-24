import React, { useState } from 'react';
import { Navbar, Button, Form, InputGroup } from 'react-bootstrap';
import { FaBars, FaSearch, FaCalendarAlt } from 'react-icons/fa';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import './Header.css';

const Header = ({ toggleSidebar }) => {
  const [selectedDate, setSelectedDate] = useState(new Date());
  
  const formatDate = (date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
  };

  const handleDateChange = (date) => {
    setSelectedDate(date);
    // 여기에서 날짜 변경 시 데이터 조회 액션을 구현할 수 있습니다
    console.log("Selected date:", formatDate(date));
  };

  return (
    <Navbar className="header-navbar" fixed="top">
      <div className="header-container">
        <div className="header-left">
          <Button 
            variant="link" 
            className="sidebar-toggle" 
            onClick={toggleSidebar}
          >
            <FaBars />
          </Button>
          <DatePicker
            selected={selectedDate}
            onChange={handleDateChange}
            dateFormat="yyyy/MM/dd"
            className="form-control date-picker"
            customInput={
              <InputGroup>
                <Form.Control 
                  value={selectedDate ? formatDate(selectedDate) : ''} 
                  readOnly 
                />
                <InputGroup.Text><FaCalendarAlt /></InputGroup.Text>
              </InputGroup>
            }
          />
        </div>
        
        <div className="header-right">
          <InputGroup className="search-bar">
            <Form.Control
              placeholder="종목 검색..."
              aria-label="종목 검색"
            />
            <Button variant="outline-secondary">
              <FaSearch />
            </Button>
          </InputGroup>
        </div>
      </div>
    </Navbar>
  );
};

export default Header;
