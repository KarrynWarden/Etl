import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// base: './' — приложение живёт на подпути (Apache отдаёт его как /dagbuilder/),
// и абсолютные ссылки на ассеты там не работают.
//
// Версии в package.json прибиты гвоздями намеренно: сборка идёт на машине с
// интернетом, а на сервер приезжает готовый dist. Плавающие версии означали бы,
// что через полгода собирается не то, что проверяли.
//
// Vite 4 (а не 5+) — потому что Rollup 4 перешёл на нативные бинарники под
// Windows 8+, а собираем мы на Windows 7. Rollup 3 внутри Vite 4 — чистый JS.
export default defineConfig({
  base: './',
  plugins: [react()],
  server: {
    // В режиме разработки API отвечает отдельным процессом:
    //   python tools/dagbuilder_api.py --port 8085
    proxy: { '/api': 'http://127.0.0.1:8085' },
  },
})
