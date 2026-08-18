import React from 'react'
import ReactDOM from 'react-dom/client'
import 'antd/dist/reset.css'

import App from './App'
import ErrorBoundary from './ErrorBoundary'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>,
)
