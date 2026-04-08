/**
 * Minimal HTTP server for Render Web Service (must bind to PORT and stay running).
 * Contract extraction runs via `npm run extract` (e.g. Render Cron or local/CI).
 */
import http from 'http';

const port = Number(process.env.PORT) || 3000;

const server = http.createServer((req, res) => {
  const url = req.url?.split('?')[0] || '/';
  if (url === '/' || url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Contract extraction service OK. Run batch via npm run extract.\n');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found\n');
});

server.listen(port, '0.0.0.0', () => {
  console.log(`Listening on 0.0.0.0:${port}`);
});
