const express = require('express');
const path = require('path');
const app = express();
const port = 3001;

// Serve static files from the 'docs' directory
app.use(express.static(path.join(__dirname, 'docs')));

// Serve the HTML file
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'docs/index.html'));
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});