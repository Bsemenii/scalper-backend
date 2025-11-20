import React from 'react';
import { BrowserRouter, Routes, Route, Link, Navigate } from 'react-router-dom';
import CryptoDashboard from './components/CryptoDashboard';
import PaperPage from './components/PaperPage';
import './App.css';

function App() {
  return (
    <BrowserRouter>
      <div className="trading-app">
        <nav
          style={{
            background: '#131722',
            borderBottom: '1px solid #1e2329',
            padding: '6px 12px',
            display: 'flex',
            gap: 12,
          }}
        >
          <Link to="/" style={{ color: '#e0e5f2', textDecoration: 'none' }}>Live</Link>
          <Link to="/paper-fast" style={{ color: '#e0e5f2', textDecoration: 'none' }}>Paper Fast</Link>
        </nav>

        <Routes>
          <Route path="/" element={<CryptoDashboard />} />
          <Route path="/paper" element={<PaperPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;
