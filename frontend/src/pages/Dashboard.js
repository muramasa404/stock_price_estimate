import React, { useState, useEffect } from 'react';
import { Row, Col, Card, Table } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import { Bar, Doughnut, Line } from 'react-chartjs-2';
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
  Legend,
  Filler
} from 'chart.js';
import './Dashboard.css';

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
  Legend,
  Filler
);

const Dashboard = () => {
  // 차트 데이터 상태 (실제로는 API에서 가져온 데이터로 설정)
  const [marketData, setMarketData] = useState({
    kospi: { current: 2580.21, change: 15.32, changeRate: 0.6 },
    kosdaq: { current: 840.13, change: -3.45, changeRate: -0.41 }
  });
  
  const [topStocks, setTopStocks] = useState([]);
  const [gradeData, setGradeData] = useState({});
  const [trendData, setTrendData] = useState({});
  
  // 샘플 데이터 (실제로는 API를 통해 데이터 로드)
  useEffect(() => {
    // 샘플 상위 종목
    setTopStocks([
      { code: '005930', name: '삼성전자', price: 72800, change: 2.1, grade: 'S', inst_rank: 1, fore_rank: 3 },
      { code: '373220', name: 'LG에너지솔루션', price: 438500, change: 1.5, grade: 'A', inst_rank: 2, fore_rank: 5 },
      { code: '000660', name: 'SK하이닉스', price: 154500, change: 0.8, grade: 'S', inst_rank: 3, fore_rank: 1 },
      { code: '207940', name: '삼성바이오로직스', price: 785000, change: -0.5, grade: 'B', inst_rank: 8, fore_rank: 12 },
      { code: '005380', name: '현대차', price: 246500, change: 1.2, grade: 'A', inst_rank: 5, fore_rank: 4 }
    ]);
    
    // 등급별 주식 수 차트 데이터
    setGradeData({
      labels: ['S등급', 'A등급', 'B등급'],
      datasets: [
        {
          data: [15, 35, 50],
          backgroundColor: ['#1e3799', '#44bd32', '#718093'],
          hoverBackgroundColor: ['#0c2461', '#20bf6b', '#57606f'],
          borderWidth: 0
        }
      ]
    });
    
    // 순매수 트렌드 차트 데이터
    setTrendData({
      labels: ['7일 전', '6일 전', '5일 전', '4일 전', '3일 전', '2일 전', '오늘'],
      datasets: [
        {
          label: '기관 순매수',
          data: [250, 320, -150, -200, 180, 400, 350],
          borderColor: '#0097e6',
          backgroundColor: 'rgba(0, 151, 230, 0.1)',
          fill: true,
          tension: 0.3
        },
        {
          label: '외국인 순매수',
          data: [180, -120, -280, 150, 320, 250, 200],
          borderColor: '#e1b12c',
          backgroundColor: 'rgba(225, 177, 44, 0.1)',
          fill: true,
          tension: 0.3
        }
      ]
    });
    
  }, []);

  return (
    <div className="dashboard-page">
      <h2 className="page-title">주식 시장 대시보드</h2>
      
      {/* 시장 개요 섹션 */}
      <Row className="mb-4">
        <Col md={6} lg={3} className="mb-3">
          <Card className="dashboard-card market-card">
            <Card.Body>
              <h4>KOSPI</h4>
              <h3 className={marketData.kospi.change > 0 ? 'price-up' : 'price-down'}>
                {marketData.kospi.current.toLocaleString()}
              </h3>
              <p className={marketData.kospi.change > 0 ? 'price-up' : 'price-down'}>
                {marketData.kospi.change > 0 ? '▲' : '▼'} {Math.abs(marketData.kospi.change).toLocaleString()} ({marketData.kospi.changeRate}%)
              </p>
            </Card.Body>
          </Card>
        </Col>
        
        <Col md={6} lg={3} className="mb-3">
          <Card className="dashboard-card market-card">
            <Card.Body>
              <h4>KOSDAQ</h4>
              <h3 className={marketData.kosdaq.change > 0 ? 'price-up' : 'price-down'}>
                {marketData.kosdaq.current.toLocaleString()}
              </h3>
              <p className={marketData.kosdaq.change > 0 ? 'price-up' : 'price-down'}>
                {marketData.kosdaq.change > 0 ? '▲' : '▼'} {Math.abs(marketData.kosdaq.change).toLocaleString()} ({marketData.kosdaq.changeRate}%)
              </p>
            </Card.Body>
          </Card>
        </Col>
        
        <Col md={12} lg={6} className="mb-3">
          <Card className="dashboard-card">
            <Card.Body>
              <h4 className="card-title">등급별 종목 분포</h4>
              <div className="chart-container">
                <Doughnut 
                  data={gradeData} 
                  options={{
                    plugins: {
                      legend: {
                        position: 'right',
                        labels: {
                          usePointStyle: true
                        }
                      }
                    },
                    cutout: '60%',
                    responsive: true,
                    maintainAspectRatio: false
                  }}
                  height={200}
                />
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
      {/* 투자자별 순매수 트렌드 */}
      <Row className="mb-4">
        <Col md={12}>
          <Card className="dashboard-card">
            <Card.Body>
              <h4 className="card-title">투자자별 순매수 트렌드 (단위: 억원)</h4>
              <div className="chart-container">
                <Line 
                  data={trendData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: {
                        position: 'top'
                      }
                    },
                    scales: {
                      y: {
                        grid: {
                          color: 'rgba(0, 0, 0, 0.05)'
                        }
                      },
                      x: {
                        grid: {
                          display: false
                        }
                      }
                    }
                  }}
                  height={300}
                />
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
      {/* 기관 및 외국인 순매수 상위 종목 */}
      <Row className="mb-4">
        <Col md={12}>
          <Card className="dashboard-card">
            <Card.Body>
              <h4 className="card-title">기관/외국인 순매수 상위 종목</h4>
              <Table responsive className="stock-table">
                <thead>
                  <tr>
                    <th>종목코드</th>
                    <th>종목명</th>
                    <th>현재가</th>
                    <th>등락률</th>
                    <th>투자등급</th>
                    <th>기관 순위</th>
                    <th>외국인 순위</th>
                  </tr>
                </thead>
                <tbody>
                  {topStocks.map(stock => (
                    <tr key={stock.code}>
                      <td>{stock.code}</td>
                      <td>
                        <Link to={`/stock/${stock.code}`}>{stock.name}</Link>
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
                    </tr>
                  ))}
                </tbody>
              </Table>
              <div className="text-end">
                <Link to="/stocks" className="btn btn-sm btn-outline-primary">더보기</Link>
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Dashboard;
