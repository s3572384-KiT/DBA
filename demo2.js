const fs = require("fs");
const fastcsv = require("fast-csv");
const mongodb = require("mongodb").MongoClient;

const path = "../count.csv";
let url = "mongodb://localhost:27017/";
let stream = fs.createReadStream(path);

// list container for 3 different entities
let dateTimeList = [];
let sensorList = [];
let countList = [];

const monthMap = new Map();
const dayMap = new Map();
const months = ["", "January", "February", "March", "April", "May", "June", "July",
	"August", "September", "October", "November", "December"];
const days = ["", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

for (let i = 1; i < months.length; ++i) {
	monthMap.set(months[i], i);
}

for (let i = 1; i < days.length; ++i) {
	dayMap.set(days[i], i);
}

monthMap.forEach((value, key) => {
	console.log(key, value);
});

dayMap.forEach((value, key) => {
	console.log(key, value);
});

let countId, dateId, sensorId;
let dateTime, year, month, mDate, day, time, sensorName, hourlyCounts;

// attribute index
const countIdIdx = 0;
const dateTimeIdx = 1;
const yearIdx = 2;
const monthIdx = 3;
const mDateIdx = 4;
const dayIdx = 5;
const timeIdx = 6;
const sensorIdIdx = 7;
const sensorNameIdx = 8;
const hourlyCountsIdx = 9;

// list index
let sensorPos = 0;
const sensorMap = new Map();

let datePos = 0;
const dateTimeMap = new Map();

const addToSensorList = (sensorId, sensorName, countId) => {
	if (!sensorMap.has(sensorId)) {
		sensorMap.set(sensorId, sensorPos++);
		sensorList.push({
			id: sensorId,
			name: sensorName,
			counts: [countId]
		});
	} else {
		let idx = sensorMap.get(sensorId);
		sensorList[idx].counts.push(countId);
	}
}

const addToDateTimeList = (dateId, datetime, year, month, mDate, day, time, countId) => {
	if (!dateTimeMap.has(dateId)) {
		dateTimeMap.set(dateId, datePos++);
		dateTimeList.push({
			id: dateId,
			desc: datetime,
			"year": year,
			"month": month,
			"mDate": mDate,
			"day": day,
			"time": time,
			counts: [countId]
		});
	} else {
		let idx = dateTimeMap.get(dateId);
		dateTimeList[idx].counts.push(countId);
	}
}

const addToCountList = (countId, hourlyCounts, dateId, sensorId) => {
	countList.push({
		id: countId,
		counts: hourlyCounts,
		"dateId": dateId,
		"sensorId": sensorId
	})
}

let count = 0;
const start = Date.now();
console.log('Job begins: loading data starting ...');

let csvStream = fastcsv
	.parse()
	.on("data", data => {
		if (count == 0) {
			count += 1;
			console.log(data);
		} else {
			// extract attributes
			countId = data[countIdIdx].trim();
			dateTime = data[dateTimeIdx].trim();
			year = data[yearIdx].trim();
			month = data[monthIdx].trim();
			mDate = data[mDateIdx].trim();
			day = data[dayIdx].trim();
			time = data[timeIdx].trim();
			sensorId = data[sensorIdIdx].trim();
			sensorName = data[sensorNameIdx].trim();
			hourlyCounts = data[hourlyCountsIdx].trim();

			// convert month, day, e.g. "February" -> 2, "Wednesday" -> 3
			month = monthMap.get(month);
			day = dayMap.get(day);

			// construct date id
			dateId = year + (month < 10 ? "0" + month : month)
				+ (mDate.length < 2 ? "0" + mDate : mDate)
				+ (time.length < 2 ? "0" + time : time);

			// convert String to int
			countId = parseInt(countId);
			dateId = parseInt(dateId);
			year = parseInt(year);
			mDate = parseInt(mDate);
			time = parseInt(time);
			sensorId = parseInt(sensorId);
			hourlyCounts = parseInt(hourlyCounts);

			// add record to different list
			addToSensorList(sensorId, sensorName, countId);
			addToDateTimeList(dateId, dateTime, year, month, mDate, day, time, countId);
			addToCountList(countId, hourlyCounts, dateId, sensorId);
		}
	})
	.on("end", () => {
		// remove the first line: header
		// csvData.shift();
		// save to the MongoDB database collection
		for (let i = 0; i < 1; ++i) {
			console.log(dateTimeList[i]);
			console.log(sensorList[i]);
			console.log(countList[i]);
		}

		console.log("date time count: ", dateTimeList.length);
		console.log("sensor count: ", sensorList.length);
		console.log("count number: ", countList.length);

		// connect to mongodb
		const client = await mongodb.connect(url, {
			useNewUrlParser: true,
			useUnifiedTopology: true
		});

		let db = "count_db";
		const dateTimeCol = "datetime";
		const sensorCol = "sensor";
		const countCol = "count";
		// specify the DB's name
		db = client.db(db);

		// execute insert query
		await db.collection(dateTimeCol).insertMany(dateTimeList, (err, res) => {
			if (err) throw err;
			console.log(`Inserted dateTimeList: ${res.insertedCount} rows`);
		});

		await db.collection(sensorCol).insertMany(sensorList, (err, res) => {
			if (err) throw err;
			console.log(`Inserted sensorList: ${res.insertedCount} rows`);
		});

		await db.collection(sensorCol).insertMany(countList, (err, res) => {
			if (err) throw err;
			console.log(`Inserted countList: ${res.insertedCount} rows`);
		});

		// close connection
		client.close();
		const duration = Date.now() - start;
		console.log('Job finished: loading data into mongodb completed ...');

		console.log(`time elapsed = ${duration} milliseconds, ${Math.floor(duration / 1000)} seconds`);
		// mongodb.connect(
		// 	url,
		// 	{ useNewUrlParser: true, useUnifiedTopology: true },
		// 	(err, client) => {
		// 		if (err) throw err;

		// 		const dateTimeCol = "datetime";
		// 		const sensorCol = "sensor";
		// 		const countCol = "count";

		// 		client
		// 			.db(db)
		// 			.collection(dateTimeCol)
		// 			.insertMany(dateTimeList, (err, res) => {
		// 				if (err) throw err;
		// 				console.log(`Inserted dateTimeList: ${res.insertedCount} rows`);
		// 			});

		// 		client
		// 			.db(db)
		// 			.collection(sensorCol)
		// 			.insertMany(sensorList, (err, res) => {
		// 				if (err) throw err;
		// 				console.log(`Inserted sensorList: ${res.insertedCount} rows`);
		// 			});

		// 		client
		// 			.db(db)
		// 			.collection(countCol)
		// 			.insertMany(countList, (err, res) => {
		// 				if (err) throw err;
		// 				console.log(`Inserted countList: ${res.insertedCount} rows`);
		// 			});

		// 		client.close();

		// 	}
		// );
	});

stream.pipe(csvStream);
