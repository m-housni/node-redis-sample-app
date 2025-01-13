// Import necessary modules
const bcrypt = require('bcrypt'); // For hashing passwords
const config = require('better-config'); // For loading configuration files

// Load the configuration file
config.set(`../../${process.env.CRASH_COURSE_CONFIG_FILE || 'config.json'}`);

// Import Redis client module
const redis = require('./redisclient');

// Get a Redis client instance
const redisClient = redis.getClient();

// Name of the consumer group for Redis Streams
const CONSUMER_GROUP_NAME = 'checkinConsumers';

// Display usage instructions
const usage = () => {
  console.error('Usage: npm run load users|locations|locationdetails|checkins|indexes|bloom|all');
  process.exit(0); // Exit the process with status 0
};

// Generic function to load data into Redis
const loadData = async (jsonArray, keyName) => {
  const pipeline = redisClient.pipeline(); // Use a pipeline for bulk operations

  // Iterate through the array of JSON objects
  for (const obj of jsonArray) {
    // Add each object to the Redis pipeline as a hash
    pipeline.hset(redis.getKeyName(keyName, obj.id), obj);
  }

  // Execute the pipeline and handle responses
  const responses = await pipeline.exec();
  let errorCount = 0;

  // Check for errors in the responses
  for (const response of responses) {
    if (response[0] !== null) {
      errorCount += 1; // Increment error count if any operation failed
    }
  }

  return errorCount; // Return the total error count
};

// Load user data into Redis
const loadUsers = async () => {
  console.log('Loading user data...');
  const usersJSON = require('../../data/users.json'); // Load users from JSON file

  // Hash user passwords before storing
  usersJSON.users.map((user) => {
    user.password = bcrypt.hashSync(user.password, 5); // Use bcrypt for password hashing
  });

  // Load user data into Redis and log the result
  const errorCount = await loadData(usersJSON.users, 'users');
  console.log(`User data loaded with ${errorCount} errors.`);
};

// Load location data into Redis
const loadLocations = async () => {
  console.log('Loading location data...');
  const locationsJSON = require('../../data/locations.json'); // Load locations from JSON file

  // Load location data into Redis and log the result
  const errorCount = await loadData(locationsJSON.locations, 'locations');
  console.log(`Location data loaded with ${errorCount} errors.`);
};

// Load location details into Redis as JSON
const loadLocationDetails = async () => {
  console.log('Loading location details...');
  const locationsJSON = require('../../data/locationdetails.json'); // Load details from JSON file

  const pipeline = redisClient.pipeline();

  // Add each location detail to the pipeline as JSON
  for (const locationDetail of locationsJSON.locationDetails) {
    pipeline.call(
      'JSON.SET',
      redis.getKeyName('locationdetails', locationDetail.id),
      '.',
      JSON.stringify(locationDetail)
    );
  }

  // Execute the pipeline and handle errors
  const responses = await pipeline.exec();
  let errorCount = 0;

  for (const response of responses) {
    if (response[0] !== null && response[1] !== 'OK') {
      errorCount += 1;
    }
  }

  console.log(`Location detail data loaded with ${errorCount} errors.`);
};

// Load check-in data as a Redis Stream
const loadCheckins = async () => {
  console.log('Loading checkin stream entries...');
  const { checkins } = require('../../data/checkins.json'); // Load check-ins from JSON file

  const streamKeyName = redis.getKeyName('checkins');

  // Delete any previous stream data
  await redisClient.del(streamKeyName);

  // Batch load check-ins into Redis Stream
  let n = 0;
  let pipeline = redisClient.pipeline();

  do {
    const checkin = checkins[n];
    pipeline.xadd(
      streamKeyName,
      checkin.id,
      'locationId',
      checkin.locationId,
      'userId',
      checkin.userId,
      'starRating',
      checkin.starRating
    );
    n += 1;

    // Execute pipeline every 100 entries
    if (n % 100 === 0) {
      await pipeline.exec();
      pipeline = redisClient.pipeline(); // Start a new pipeline
    }
  } while (n < checkins.length);

  // Execute remaining commands in the pipeline
  if (pipeline.length > 0) {
    await pipeline.exec();
  }

  // Log the total number of entries loaded
  const numEntries = await redisClient.xlen(streamKeyName);
  console.log(`Loaded ${numEntries} checkin stream entries.`);

  // Create a consumer group for the stream
  console.log('Creating consumer group...');
  pipeline = redisClient.pipeline();
  pipeline.xgroup('DESTROY', streamKeyName, CONSUMER_GROUP_NAME); // Delete existing group if any
  pipeline.xgroup('CREATE', streamKeyName, CONSUMER_GROUP_NAME, 0); // Create new group
  await pipeline.exec();
  console.log('Consumer group created.');
};

// Create indexes for searching users and locations
const createIndexes = async () => {
  console.log('Dropping any existing indexes, creating new indexes...');

  const usersIndexKey = redis.getKeyName('usersidx');
  const locationsIndexKey = redis.getKeyName('locationsidx');

  const pipeline = redisClient.pipeline();

  // Drop and recreate indexes with specific schemas
  pipeline.call('FT.DROPINDEX', usersIndexKey);
  pipeline.call('FT.DROPINDEX', locationsIndexKey);
  pipeline.call(
    'FT.CREATE',
    usersIndexKey,
    'ON',
    'HASH',
    'PREFIX',
    '1',
    redis.getKeyName('users'),
    'SCHEMA',
    'email',
    'TAG',
    'numCheckins',
    'NUMERIC',
    'SORTABLE',
    'lastSeenAt',
    'NUMERIC',
    'SORTABLE',
    'lastCheckin',
    'NUMERIC',
    'SORTABLE',
    'firstName',
    'TEXT',
    'lastName',
    'TEXT'
  );
  pipeline.call(
    'FT.CREATE',
    locationsIndexKey,
    'ON',
    'HASH',
    'PREFIX',
    '1',
    redis.getKeyName('locations'),
    'SCHEMA',
    'category',
    'TAG',
    'SORTABLE',
    'location',
    'GEO',
    'SORTABLE',
    'numCheckins',
    'NUMERIC',
    'SORTABLE',
    'numStars',
    'NUMERIC',
    'SORTABLE',
    'averageStars',
    'NUMERIC',
    'SORTABLE'
  );

  const responses = await pipeline.exec();

  if (responses.length === 4 && responses[2][1] === 'OK' && responses[3][1] === 'OK') {
    console.log('Created indexes.');
  } else {
    console.log('Unexpected error creating indexes :(');
    console.log(responses);
  }
};

// Create a Bloom filter for check-ins
const createBloomFilter = async () => {
  console.log('Deleting any previous bloom filter, creating new bloom filter...');

  const bloomFilterKey = redis.getKeyName('checkinfilter');

  const pipeline = redisClient.pipeline();
  pipeline.del(bloomFilterKey); // Delete existing Bloom filter
  pipeline.call('BF.RESERVE', bloomFilterKey, 0.0001, 1000000); // Create a new Bloom filter

  const responses = await pipeline.exec();

  if (responses.length === 2 && responses[1][1] === 'OK') {
    console.log('Created bloom filter.');
  } else {
    console.log('Unexpected error creating bloom filter :(');
    console.log(responses);
  }
};

// Main function to determine which data to load
const runDataLoader = async (params) => {
  if (params.length !== 4) {
    usage(); // Show usage instructions if arguments are incorrect
  }

  const command = params[3]; // Get the command argument

  // Determine which action to perform based on the command
  switch (command) {
    case 'users':
      await loadUsers();
      break;
    case 'locations':
      await loadLocations();
      break;
    case 'locationdetails':
      await loadLocationDetails();
      break;
    case 'checkins':
      await loadCheckins();
      break;
    case 'indexes':
      await createIndexes();
      break;
    case 'bloom':
      await createBloomFilter();
      break;
    case 'all':
      await loadUsers();
      await loadLocations();
      await loadLocationDetails();
      await loadCheckins();
      await createIndexes();
      await createBloomFilter();
      break;
    default:
      usage();
  }

  redisClient.quit(); // Close Redis connection
};

// Run the data loader with the provided arguments
runDataLoader(process.argv);
