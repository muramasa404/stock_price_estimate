import React, { useState, useEffect } from 'react';
import { 
  Row, 
  Col, 
  Card, 
  Table, 
  Form, 
  InputGroup, 
  Button, 
  Pagination,
  Badge
} from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { FaSearch, FaFilter, FaSortAmountDown, FaSortAmountUp } from 'react-icons/fa';
import './StockList.css';

const StockList = () => {
  // 상태 관리
  const [stocks, setStocks] = useState([]);
  const [filteredStocks, setFilteredStocks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({
    key: 'inst_rank',
    direction: 'asc'
  });
  const [filterGrade, setFilterGrade] = useState('all');
  
  const itemsPerPage = 15;

  // 샘플 데이터 로드
  useEffect(() => {
    setLoading(true);
    // 실제로는 API 호출
    const fetchData = async () => {
      try {
        // 샘플 데이터
        const sampleStocks = Array(50).fill().map((_, index) => {
          const grades = ['S', 'A', 'B'];
          const randomGrade = grades[Math.floor(Math.random() * grades.length)];
          const randomChange = (Math.random() * 6 - 3).toFixed(2);
          const basePrice = 50000 + Math.floor(Math.random() * 150000);
          
          return {
            code: `00${(index + 1000).toString().slice(1)}`,
            name: `샘플종목${index + 1}`,
            price: basePrice,
            change: parseFloat(randomChange),
            grade: randomGrade,
            inst_rank: index + 1,
            fore_rank: Math.floor(Math.random() * 50) + 1,
            volume: Math.floor(Math.random() * 1000000) + 100000,
            market: Math.random() > 0.5 ? 'KOSPI' : 'KOSDAQ',
          };
        });
        
        setStocks(sampleStocks);
        setFilteredStocks(sampleStocks);
        setLoading(false);
      } catch (error) {
        console.error('데이터 로드 중 오류 발생:', error);
        setLoading(false);
      }
    };
    
    fetchData();
  }, []);

  // 필터 및 검색 적용
  useEffect(() => {
    let result = [...stocks];
    
    // 검색어 필터링
    if (searchTerm) {
      result = result.filter(
        stock => 
          stock.code.includes(searchTerm) || 
          stock.name.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }
    
    // 등급 필터링
    if (filterGrade !== 'all') {
      result = result.filter(stock => stock.grade === filterGrade);
    }
    
    // 정렬 적용
    if (sortConfig.key) {
      result.sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? -1 : 1;
        }
        if (a[sortConfig.key] > b[sortConfig.key]) {
          return sortConfig.direction === 'asc' ? 1 : -1;
        }
        return 0;
      });
    }
    
    setFilteredStocks(result);
    setCurrentPage(1); // 필터링 후 첫 페이지로 이동
  }, [searchTerm, filterGrade, sortConfig, stocks]);

  // 정렬 처리
  const requestSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  // 페이지네이션 로직
  const indexOfLastItem = currentPage * itemsPerPage;
  const indexOfFirstItem = indexOfLastItem - itemsPerPage;
  const currentStocks = filteredStocks.slice(indexOfFirstItem, indexOfLastItem);
  const totalPages = Math.ceil(filteredStocks.length / itemsPerPage);

  // 페이지네이션 아이템 생성
  const paginationItems = [];
  for (let number = 1; number <= totalPages; number++) {
    if (
      number === 1 || 
      number === totalPages || 
      (number >= currentPage - 1 && number <= currentPage + 1)
    ) {
      paginationItems.push(
        <Pagination.Item 
          key={number} 
          active={number === currentPage}
          onClick={() => setCurrentPage(number)}
        >
          {number}
        </Pagination.Item>
      );
    } else if (
      (number === currentPage - 2 && currentPage > 3) ||
      (number === currentPage + 2 && currentPage < totalPages - 2)
    ) {
      paginationItems.push(<Pagination.Ellipsis key={`ellipsis-${number}`} />);
    }
  }

  return (
    <div className="stock-list-page">
      <h2 className="page-title">종목 리스트</h2>
      
      {/* 필터 및 검색 영역 */}
      <Card className="filter-card mb-4">
        <Card.Body>
          <Row>
            <Col md={6} lg={4} className="mb-3">
              <InputGroup>
                <Form.Control
                  placeholder="종목명 또는 코드 검색..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
                <Button variant="outline-secondary">
                  <FaSearch />
                </Button>
              </InputGroup>
            </Col>
            
            <Col md={6} lg={3} className="mb-3">
              <InputGroup>
                <InputGroup.Text><FaFilter /></InputGroup.Text>
                <Form.Select
                  value={filterGrade}
                  onChange={(e) => setFilterGrade(e.target.value)}
                >
                  <option value="all">전체 등급</option>
                  <option value="S">S등급</option>
                  <option value="A">A등급</option>
                  <option value="B">B등급</option>
                </Form.Select>
              </InputGroup>
            </Col>
          </Row>
        </Card.Body>
      </Card>
      
      {/* 종목 테이블 */}
      <Card className="dashboard-card">
        <Card.Body>
          <Table responsive className="stock-table">
            <thead>
              <tr>
                <th>종목코드</th>
                <th>종목명</th>
                <th>시장</th>
                <th onClick={() => requestSort('price')} className="sortable">
                  현재가
                  {sortConfig.key === 'price' && (
                    sortConfig.direction === 'asc' ? <FaSortAmountUp className="ms-1" /> : <FaSortAmountDown className="ms-1" />
                  )}
                </th>
                <th onClick={() => requestSort('change')} className="sortable">
                  등락률
                  {sortConfig.key === 'change' && (
                    sortConfig.direction === 'asc' ? <FaSortAmountUp className="ms-1" /> : <FaSortAmountDown className="ms-1" />
                  )}
                </th>
                <th onClick={() => requestSort('grade')} className="sortable">
                  투자등급
                  {sortConfig.key === 'grade' && (
                    sortConfig.direction === 'asc' ? <FaSortAmountUp className="ms-1" /> : <FaSortAmountDown className="ms-1" />
                  )}
                </th>
                <th onClick={() => requestSort('inst_rank')} className="sortable">
                  기관순위
                  {sortConfig.key === 'inst_rank' && (
                    sortConfig.direction === 'asc' ? <FaSortAmountUp className="ms-1" /> : <FaSortAmountDown className="ms-1" />
                  )}
                </th>
                <th onClick={() => requestSort('fore_rank')} className="sortable">
                  외국인순위
                  {sortConfig.key === 'fore_rank' && (
                    sortConfig.direction === 'asc' ? <FaSortAmountUp className="ms-1" /> : <FaSortAmountDown className="ms-1" />
                  )}
                </th>
                <th onClick={() => requestSort('volume')} className="sortable">
                  거래량
                  {sortConfig.key === 'volume' && (
                    sortConfig.direction === 'asc' ? <FaSortAmountUp className="ms-1" /> : <FaSortAmountDown className="ms-1" />
                  )}
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan="9" className="text-center">데이터를 불러오는 중...</td>
                </tr>
              ) : currentStocks.length > 0 ? (
                currentStocks.map(stock => (
                  <tr key={stock.code}>
                    <td>{stock.code}</td>
                    <td>
                      <Link to={`/stock/${stock.code}`}>{stock.name}</Link>
                    </td>
                    <td>
                      <Badge bg={stock.market === 'KOSPI' ? 'primary' : 'success'} className="market-badge">
                        {stock.market}
                      </Badge>
                    </td>
                    <td>{stock.price.toLocaleString()} 원</td>
                    <td className={stock.change > 0 ? 'price-up' : 'price-down'}>
                      {stock.change > 0 ? '+' : ''}{stock.change}%
                    </td>
                    <td>
                      <span className={`grade grade-${stock.grade.toLowerCase()}`}>
                        {stock.grade}
                      </span>
                    </td>
                    <td>{stock.inst_rank}</td>
                    <td>{stock.fore_rank}</td>
                    <td>{stock.volume.toLocaleString()}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan="9" className="text-center">검색 결과가 없습니다.</td>
                </tr>
              )}
            </tbody>
          </Table>
          
          {/* 페이지네이션 */}
          {filteredStocks.length > itemsPerPage && (
            <div className="d-flex justify-content-center mt-4">
              <Pagination>
                <Pagination.First onClick={() => setCurrentPage(1)} disabled={currentPage === 1} />
                <Pagination.Prev onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))} disabled={currentPage === 1} />
                {paginationItems}
                <Pagination.Next onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))} disabled={currentPage === totalPages} />
                <Pagination.Last onClick={() => setCurrentPage(totalPages)} disabled={currentPage === totalPages} />
              </Pagination>
            </div>
          )}
        </Card.Body>
      </Card>
    </div>
  );
};

export default StockList;
