import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { Row, Col, Card, Table, Badge } from 'react-bootstrap';
import { Line } from 'react-chartjs-2';
import { 
  Chart as ChartJS, 
  CategoryScale, 
  LinearScale, 
  PointElement, 
  LineElement,
  Title, 
  Tooltip, 
  Legend,
  Filler
} from 'chart.js';
import './StockDetail.css';

// Chart.js 등록
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const StockDetail = () => {
  const { stockCode } = useParams();
  const [stockData, setStockData] = useState(null);
  const [priceHistory, setPriceHistory] = useState({});
  const [volumeHistory, setVolumeHistory] = useState({});
  const [investorData, setInvestorData] = useState([]);
  const [loading, setLoading] = useState(true);
  
  // 샘플 데이터 로드 (실제로는 API 호출)
  useEffect(() => {
    const fetchStockDetail = async () => {
      try {
        setLoading(true);
        
        // 임시 데이터 생성
        const stockNames = {
          '005930': '삼성전자',
          '373220': 'LG에너지솔루션',
          '000660': 'SK하이닉스',
          '207940': '삼성바이오로직스',
          '005380': '현대차'
        };
        
        const stockName = stockNames[stockCode] || `종목 ${stockCode}`;
        
        // 주가 차트 데이터 생성 (최근 30일)
        const dates = Array(30).fill().map((_, i) => {
          const date = new Date();
          date.setDate(date.getDate() - (29 - i));
          return date.toLocaleDateString('ko-KR', { month: 'short', day: 'numeric' });
        });
        
        const basePrice = stockCode === '005930' ? 72000 : 
                         stockCode === '373220' ? 430000 : 
                         stockCode === '000660' ? 150000 : 
                         stockCode === '207940' ? 780000 : 
                         stockCode === '005380' ? 245000 : 50000;
        
        // 주가 랜덤 생성
        const prices = [];
        let price = basePrice;
        for (let i = 0; i < 30; i++) {
          price = price + (Math.random() * 2000 - 1000);
          prices.push(Math.round(price));
        }
        
        // 거래량 랜덤 생성
        const volumes = Array(30).fill().map(() => Math.floor(Math.random() * 5000000) + 1000000);
        
        // 투자자별 순매수 데이터
        const investorTypes = ['기관', '외국인', '개인', '연기금'];
        const investorBuySellData = investorTypes.map(type => {
          const buyAmount = Math.floor(Math.random() * 5000) + 1000;
          const sellAmount = Math.floor(Math.random() * 5000) + 1000;
          const netAmount = buyAmount - sellAmount;
          
          return {
            investorType: type,
            buyAmount,
            sellAmount,
            netAmount
          };
        });
        
        // 주식 기본정보
        const currentPrice = prices[prices.length - 1];
        const prevPrice = prices[prices.length - 2];
        const change = currentPrice - prevPrice;
        const changeRate = ((change / prevPrice) * 100).toFixed(2);
        
        const stockInfo = {
          code: stockCode,
          name: stockName,
          price: currentPrice,
          change,
          changeRate: parseFloat(changeRate),
          high: Math.max(...prices.slice(-5)),
          low: Math.min(...prices.slice(-5)),
          volume: volumes[volumes.length - 1],
          market: Math.random() > 0.5 ? 'KOSPI' : 'KOSDAQ',
          grade: Math.random() > 0.7 ? 'S' : Math.random() > 0.4 ? 'A' : 'B',
          instRank: Math.floor(Math.random() * 100) + 1,
          foreRank: Math.floor(Math.random() * 100) + 1,
        };
        
        setPriceHistory({
          labels: dates,
          datasets: [
            {
              label: '주가',
              data: prices,
              borderColor: '#2980b9',
              backgroundColor: 'rgba(41, 128, 185, 0.1)',
              tension: 0.4,
              fill: true
            }
          ]
        });
        
        setVolumeHistory({
          labels: dates,
          datasets: [
            {
              label: '거래량',
              data: volumes,
              backgroundColor: '#3498db',
              borderRadius: 5
            }
          ]
        });
        
        setStockData(stockInfo);
        setInvestorData(investorBuySellData);
        setLoading(false);
      } catch (error) {
        console.error('주식 데이터 로드 중 오류:', error);
        setLoading(false);
      }
    };
    
    fetchStockDetail();
  }, [stockCode]);
  
  if (loading || !stockData) {
    return <div className="loading-container">데이터를 불러오는 중...</div>;
  }

  return (
    <div className="stock-detail-page">
      <h2 className="page-title">
        <span>{stockData.name}</span>
        <span className="stock-code ml-2">{stockData.code}</span>
        <Badge 
          bg={stockData.market === 'KOSPI' ? 'primary' : 'success'} 
          className="market-badge ms-2"
        >
          {stockData.market}
        </Badge>
      </h2>
      
      <Row className="mb-4">
        <Col md={6}>
          <Card className="dashboard-card stock-summary-card">
            <Card.Body>
              <div className="price-info">
                <h2 className={stockData.change > 0 ? 'price-up' : 'price-down'}>
                  {stockData.price.toLocaleString()} 원
                </h2>
                <div className={stockData.change > 0 ? 'price-up' : 'price-down'}>
                  {stockData.change > 0 ? '▲' : '▼'} {Math.abs(stockData.change).toLocaleString()} ({stockData.changeRate}%)
                </div>
              </div>
              
              <Table className="stock-info-table">
                <tbody>
                  <tr>
                    <td>고가</td>
                    <td>{stockData.high.toLocaleString()} 원</td>
                    <td>저가</td>
                    <td>{stockData.low.toLocaleString()} 원</td>
                  </tr>
                  <tr>
                    <td>거래량</td>
                    <td>{stockData.volume.toLocaleString()}</td>
                    <td>투자등급</td>
                    <td>
                      <span className={`grade grade-${stockData.grade.toLowerCase()}`}>
                        {stockData.grade}
                      </span>
                    </td>
                  </tr>
                  <tr>
                    <td>기관 순위</td>
                    <td>{stockData.instRank}위</td>
                    <td>외국인 순위</td>
                    <td>{stockData.foreRank}위</td>
                  </tr>
                </tbody>
              </Table>
            </Card.Body>
          </Card>
        </Col>
        
        <Col md={6}>
          <Card className="dashboard-card">
            <Card.Body>
              <h5 className="card-title">투자자별 매매 현황</h5>
              <Table className="investor-table">
                <thead>
                  <tr>
                    <th>투자자</th>
                    <th>순매수</th>
                    <th>매수</th>
                    <th>매도</th>
                  </tr>
                </thead>
                <tbody>
                  {investorData.map((item, index) => (
                    <tr key={index}>
                      <td>{item.investorType}</td>
                      <td className={item.netAmount > 0 ? 'price-up' : 'price-down'}>
                        {item.netAmount > 0 ? '+' : ''}
                        {item.netAmount.toLocaleString()}
                      </td>
                      <td>{item.buyAmount.toLocaleString()}</td>
                      <td>{item.sellAmount.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
      <Row className="mb-4">
        <Col md={12}>
          <Card className="dashboard-card">
            <Card.Body>
              <h5 className="card-title">주가 차트</h5>
              <div className="chart-container">
                <Line
                  data={priceHistory}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: {
                        display: false
                      },
                      tooltip: {
                        mode: 'index',
                        intersect: false
                      }
                    },
                    scales: {
                      y: {
                        ticks: {
                          callback: function(value) {
                            return value.toLocaleString() + '원';
                          }
                        },
                        grid: {
                          color: 'rgba(0, 0, 0, 0.05)'
                        }
                      },
                      x: {
                        grid: {
                          display: false
                        }
                      }
                    },
                    interaction: {
                      mode: 'nearest',
                      axis: 'x',
                      intersect: false
                    }
                  }}
                  height={300}
                />
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>
      
    </div>
  );
};

export default StockDetail;
