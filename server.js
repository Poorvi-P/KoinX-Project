const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');

const app = express();
const port = 3000;

// Middleware to parse JSON bodies
app.use(express.json());

// Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/trades');

// Define a schema and model for the trades
const tradeSchema = new mongoose.Schema({
    userId: Number,
    utcTime: Date,
    operation: String,
    market: String,
    buySellAmount: Number,
    price: Number,
});

const Trade = mongoose.model('Trade', tradeSchema);

// Set up Multer for file uploads
const upload = multer({ dest: 'uploads/' });

app.post('/upload', upload.single('file'), (req, res) => {
    const filePath = path.join(__dirname, req.file.path);
    
    const trades = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
            const utcTimeStr = row['UTC_Time'];
            // console.log(utcTimeStr);
            const dateTimeStr = utcTimeStr;
            const [datePart, timePart] = dateTimeStr.split(' ');
            const [day, month, year] = datePart.split('-').map(Number);
            const [hours, minutes] = timePart.split(':').map(Number);

            // Month in JavaScript Date object is zero-based (0 for January)
            const utcTime = new Date(year, month - 1, day, hours, minutes);

            console.log(utcTime); // Output: Mon Sep 26 2022 11:21:00 GMT+0000 (Coordinated Universal Time)

            
            trades.push({
                userId: parseInt(row['User_ID']),
                utcTime: utcTime,
                operation: row['Operation'],
                market: row['Market'],
                buySellAmount: parseFloat(row['Buy/Sell Amount']),
                price: parseFloat(row['Price']),
            });
        })
        .on('end', async () => {
            await Trade.insertMany(trades);
            fs.unlinkSync(filePath); // Remove the uploaded file
            res.send('File uploaded and data saved to database!');
        });
});
  
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});