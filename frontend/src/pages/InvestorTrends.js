import React, { useState, useEffect } from 'react';
import { Row, Col, Card, Form, Button } from 'react-bootstrap';
import { 
  Chart as ChartJS, 
  CategoryScale, 
  LinearScale, 
  BarElement, 
  LineElement,
  PointElement,
  ArcElement,
  Title, 
  Tooltip, 
  Legend
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';
import DatePicker from 'react-datepicker';
import './InvestorTrends.css';

// Chart.js 등록
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
);

const InvestorTrends = () => {
  const [startDate, setStartDate] = useState(() => {
    const date = new Date();
    date.setDate(date.getDate() - 30); // 기본 30일 전
    return date;
  });
  const [endDate, setEndDate] = useState(new Date());
  const [investorType, setInvestorType] = useState('all');
  const [trendData, setTrendData] = useState({});
  const [topStocks, setTopStocks] = useState({});
  const [loading, setLoading] = useState(false);

  // 날짜 형식 변환 함수
  const formatDate = (date) => {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
  };

  // 데이터 로드 (실제로는 API 호출)
  const loadData = () => {
    setLoading(true);
    
    // 샘플 데이터 생성
    setTimeout(() => {
      // 날짜 범위 생성
      const dateRange = [];
      const currentDate = new Date(startDate);
      while (currentDate <= endDate) {
        dateRange.push(new Date(currentDate).toLocaleDateString('ko-KR', { month: 'short', day: 'numeric' }));
        currentDate.setDate(currentDate.getDate() + 1);
      }
      
      // 투자자별 데이터 생성
      const generateInvestorData = () => {
        return dateRange.map(() => Math.floor(Math.random() * 4000) - 2000);
      };
      
      // 순매수 트렌드 차트 데이터
      setTrendData({
        labels: dateRange,
        datasets: [
          {
            label: '기관 투자자',
            data: generateInvestorData(),
            backgroundColor: 'rgba(41, 128, 185, 0.6)',
            borderColor: '#2980b9',
            borderWidth: 1
          },
          {
            label: '외국인 투자자',
            data: generateInvestorData(),
            backgroundColor: 'rgba(230, 126, 34, 0.6)',
            borderColor: '#e67e22',
            borderWidth: 1
          },
          {
            label: '개인 투자자',
            data: generateInvestorData(),
            backgroundColor: 'rgba(46, 204, 113, 0.6)',
            borderColor: '#2ecc71',
            borderWidth: 1
          }
        ]
      });
      
      // 투자자별 상위 종목
      const investorTypes = ['기관', '외국인', '개인'];
      const topStocksData = {};
      
      investorTypes.forEach(type => {
        const stocks = [];
        for (let i = 0; i < 10; i++) {
          const isBuy = Math.random() > 0.3;
          stocks.push({
            rank: i + 1,
            code: `00${(i + 1000).toString().slice(1)}`,
            name: `샘플종목${i + 1}`,
            amount: Math.floor(Math.random() * 5000) + 100,
            isBuy
          });
        }
        topStocksData[type] = stocks;
      });
      
      setTopStocks(topStocksData);
      setLoading(false);
    }, 800);
  };
  
  // 초기 로드
  useEffect(() => {
    loadData();
  }, []);

  // 검색 버튼 클릭 핸들러
  const handleSearch = (e) => {
    e.preventDefault();
    loadData();
  };

  return (
    <div className="investor-trends-page">
      <h2 className="page-title">투자자 동향 분석</h2>
      
      {/* 필터 영역 */}
      <Card className="dashboard-card filter-card mb-4">
        <Card.Body>
          <Form onSubmit={handleSearch}>
            <Row className="align-items-end">
              <Col md={3} className="mb-3">
                <Form.Group>
                  <Form.Label>시작일</Form.Label>
                  <DatePicker
                    selected={startDate}
                    onChange={date => setStartDate(date)}
                    selectsStart
                    startDate={startDate}
                    endDate={endDate}
                    maxDate={endDate}
                    className="form-control"
                    dateFormat="yyyy/MM/dd"
                  />
                </Form.Group>
              </Col>
              
              <Col md={3} className="mb-3">
                <Form.Group>
                  <Form.Label>종료일</Form.Label>
                  <DatePicker
                    selected={endDate}
                    onChange={date => setEndDate(date)}
                    selectsEnd
                    startDate={startDate}
                    endDate={endDate}
                    minDate={startDate}
                    maxDate={new Date()}
                    className="form-control"
                    dateFormat="yyyy/MM/dd"
                  />
                </Form.Group>
              </Col>
              
              <Col md={3} className="mb-3">
                <Form.Group>
                  <Form.Label>투자자 유형</Form.Label>
                  <Form.Select 
                    value={investorType}
                    onChange={e => setInvestorType(e.target.value)}
                  >
                    <option value="all">전체</option>
                    <option value="institutional">기관</option>
                    <option value="foreign">외국인</option>
                    <option value="individual">개인</option>
                  </Form.Select>
                </Form.Group>
              </Col>
              
              <Col md={3} className="mb-3">
                <Button 
                  type="submit" 
                  variant="primary" 
                  className="w-100"
                  disabled={loading}
                >
                  {loading ? '데이터 로딩 중...' : '검색'}
                </Button>
              </Col>
            </Row>
          </Form>
        </Card.Body>
      </Card>
      
      {/* 투자자별 순매수 트렌드 차트 */}
      <Card className="dashboard-card mb-4">
        <Card.Body>
          <h5 className="card-title">투자자별 순매수 추이 (단위: 억원)</h5>
          <div className="chart-container">
            {trendData.labels && (
              <Bar
                data={trendData}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  plugins: {
                    tooltip: {
                      callbacks: {
                        label: function(context) {
                          const value = context.parsed.y;
                          return `${context.dataset.label}: ${value > 0 ? '+' : ''}${value.toLocaleString()} 억원`;
                        }
                      }
                    },
                    legend: {
                      position: 'top',
                    },
                  },
                  scales: {
                    x: {
                      grid: {
                        display: false
                      }
                    },
                    y: {
                      grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                      },
                      ticks: {
                        callback: function(value) {
                          return value.toLocaleString();
                        }
                      }
                    }
                  }
                }}
                height={400}
              />
            )}
          </div>
        </Card.Body>
      </Card>
      
      {/* 투자자별 순매수 상위 종목 */}
      <Row>
        {Object.entries(topStocks).map(([type, stocks]) => (
          <Col md={4} key={type} className="mb-4">
            <Card className="dashboard-card h-100">
              <Card.Body>
                <h5 className="card-title">{type} 순매수 상위 종목</h5>
                <div className="top-stocks-list">
                  {stocks.map((stock, index) => (
                    <div key={index} className="stock-item">
                      <div className="stock-rank">{stock.rank}</div>
                      <div className="stock-info">
                        <div className="stock-name">{stock.name}</div>
                        <div className="stock-code">{stock.code}</div>
                      </div>
                      <div className={`stock-amount ${stock.isBuy ? 'buy' : 'sell'}`}>
                        {stock.isBuy ? '+' : '-'}{stock.amount.toLocaleString()}
                      </div>
                    </div>
                  ))}
                </div>
              </Card.Body>
            </Card>
          </Col>
        ))}
      </Row>
      
    </div>
  );
};

export default InvestorTrends;
