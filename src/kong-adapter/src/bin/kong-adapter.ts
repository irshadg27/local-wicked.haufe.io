'use strict';

/**
 * Module dependencies.
 */

import app from '../app';
const { debug, info, warn, error } = require('portal-env').Logger('kong-adapter:kong-adapter');
const http = require('http');
const async = require('async');
const axios = require('axios');

// On Demand Resync Changes : Start
const fs = require('fs');
const path = require('path');
var watcherDebouceTimeout;
var watcherRetryTimeout;
var watcherChanges = [];
const watcherDebouceTime = 10000; // 10 seconds
const staticConfigFolder =  process.env.PORTAL_API_STATIC_CONFIG
// On Demand Resync Changes : End


import * as wicked from 'wicked-sdk';

import { kongMain } from '../kong/main';
import { portal } from '../kong/portal';
import * as utils from '../kong/utils';
import { kongMonitor } from '../kong/monitor';

/**
 * Get port from environment and store in Express.
 */

const port = normalizePort(process.env.PORT || '3002');
app.set('port', port);

// Create HTTP server.
const server = http.createServer(app);

// Listen on provided port, on all network interfaces.
server.listen(port);
server.on('error', onError);
server.on('listening', onListening);

info('Waiting for API to be available.');

app.apiAvailable = false;
app.kongAvailable = false;

const wickedOptions = {
    userAgentName: 'wicked.portal-kong-adapter',
    userAgentVersion: utils.getVersion()
};

async.series([
    callback => wicked.initialize(wickedOptions, callback),
    callback => wicked.initMachineUser('kong-adapter', callback),
    callback => wicked.awaitUrl(wicked.getInternalKongAdminUrl(), null, callback),
    callback => utils.initGroups(callback),
    callback => kongMonitor.init(callback)
], function (err) {
    debug('Kong and API await finished.');
    if (err) {
        error('Failed waiting for API and/or Kong.');
        throw err;
    }

    // Jot down a couple of URLs
    utils.setMyUrl(wicked.getInternalKongAdapterUrl());

    // Now let's register with the portal API; we'll use the standard Admin
    const initOptions = {
        initGlobals: true,
        syncApis: false,
        syncConsumers: false
    };
    detectChangedApis(staticConfigFolder,initOptions);
    kongMain.init(initOptions, function (err) {
        debug('kong.init() returned.');
        if (err) {
            error('Could not initialize Kong adapter.');
            throw err;
        }

        // Graceful shutdown
        process.on('SIGINT', function () {
            debug("Gracefully shutting down.");
            kongMain.deinit(function (err) {
                process.exit();
            });
        });

        info("Kong Adapter initialization done.");
        app.initialized = true;

        // enable file watcher after first initialization
        info(`wicked-config Watcher: Watching for changes in ${staticConfigFolder}`);
        watchDirectory(staticConfigFolder);
    });
});

// On Demand Resync Changes : Start
// Watch for changes in any file and trigger resync after a debounce of 10s
info(`wicked-config Watcher: Watching for changes in :${staticConfigFolder}`);

let watchDirectory = (directory) => {
    // Watch the directory itself
    if (!fs.existsSync(directory)) {
        warn(`wicked-config Watcher: Directory is unavailable: ${directory}; retrying in 10 seconds.`);
        clearTimeout(watcherRetryTimeout);
        watcherRetryTimeout = setTimeout(() => watchDirectory(directory), watcherDebouceTime);
        return;
    }try{
    fs.watch(directory, (eventType, fileName) => {
        info(`wicked-config Watcher: Detected change in :${fileName} , event: ${eventType}`);
        if (fileName == "config.json") {
            let fullPath=path.join(directory, fileName);
            const apiFolderName = fullPath.split("/").slice(-2, -1)[0];
            watcherChanges.push(apiFolderName);
        } else {
            watcherChanges.push(fileName);
        }
        info('wicked-config Watcher: Waiting for more changes to arrive..');
        clearTimeout(watcherDebouceTimeout);
        watcherDebouceTimeout = setTimeout(() => {
            startResync();
        }, watcherDebouceTime);
    });
    } catch (err) {
        error(`wicked-config Watcher: Failed to watch ${directory}.`);
        error(err);
        clearTimeout(watcherRetryTimeout);
        watcherRetryTimeout = setTimeout(() => watchDirectory(directory), watcherDebouceTime);
        return;
    }
    // Watch all files and subdirectories in the directory
    fs.readdir(directory, (err, files) => {
      if (err) {
        console.error(`Error reading directory ${directory}: ${err}`);
        return;
      }
      files.forEach(file => {
        const fullPath = path.join(directory, file);
        // Check if it's a directory, and if so, watch it recursively
        if (fs.statSync(fullPath).isDirectory()) {
          watchDirectory(fullPath);
        }
      });
    });
  }

function startResync(){
    debug('Kong-Adapter File Watcher: Starting Resync for changes in :');
    const changedFiles = Array.from(new Set(watcherChanges));
    watcherChanges = [];
            for(let file of changedFiles){
                if (file && (file.includes('apis.json') || file.includes('plans.json'))) {
                    // triger wicked api restart
                    debug('detected the apis.json change, restarting the api component')
                    let localKeyEnv = "$PORTAL_LOCAL_KEY"
                    let envVarName = localKeyEnv.substring(1);
                    let localKey = process.env[envVarName]
                    const headers = {"x-local-key" : localKey};
                    let response = axios.post(`http://localhost:3001/kill`,null,{headers});
                    debug('restarted the api component');
                    setTimeout(function () {
                        process.exit(0);
                    }, 3000);
                    return;
                }
            }
    if(changedFiles.length > 0) {
       portal.refreshChangedApiConfigs(changedFiles, function (err) {
           if (err) {
               error('Failed to refresh changed API configurations.');
               error(err);
               return;
           }
           kongMain.resyncApis(changedFiles);
       });
    }
}
// On Demand Resync Changes : End

/**
 * Normalize a port into a number, string, or false.
 */

function normalizePort(val) {
    const port = parseInt(val, 10);

    if (isNaN(port)) {
        // named pipe
        return val;
    }

    if (port >= 0) {
        // port number
        return port;
    }

    return false;
}

/**
 * Event listener for HTTP server "error" event.
 */

function onError(err) {
    if (err.syscall !== 'listen') {
        throw err;
    }

    const bind = typeof port === 'string' ?
        'Pipe ' + port :
        'Port ' + port;

    // handle specific listen errors with friendly messages
    switch (err.code) {
        case 'EACCES':
            error(bind + ' requires elevated privileges');
            process.exit(1);
            break;
        case 'EADDRINUSE':
            error(bind + ' is already in use');
            process.exit(1);
            break;
        default:
            throw err;
    }
}

/**
 * Event listener for HTTP server "listening" event.
 */

function onListening() {
    const addr = server.address();
    const bind = typeof addr === 'string' ?
        'pipe ' + addr :
        'port ' + addr.port;
    debug('Listening on ' + bind);
}


function detectChangedApis(rootFolder, initOptions) {
    try {
        debug("inside detectChangedApis");
        const currentTime = new Date();
        let changedApis = [];

        function isFileChanged(filePath, currentTime, timeThreshold) {
            if (!fs.existsSync(filePath)) {
                warn(`wicked-config Watcher: Path is unavailable: ${filePath}`);
                return false;
            }
            try {
                const stats = fs.statSync(filePath);
                const lastModifiedTime = stats.mtime;
                const timeDifference = currentTime.getTime() - lastModifiedTime.getTime();
                const diffInMinutes = timeDifference / (1000 * 60);
                return diffInMinutes <= timeThreshold;
            } catch (err) {
                warn(`wicked-config Watcher: Could not read path: ${filePath}`);
                return false;
            }
        }

        const rootFolderChanged = isFileChanged(rootFolder, currentTime, 60);
        if (rootFolderChanged) {
            debug("No sync, It might be a new dev portal relase as root folder changed in last 30 minutes");
            return;
        }

        const plansPath = path.join(rootFolder, 'plans', 'plans.json');
        const plansChanged = isFileChanged(plansPath, currentTime, 10);
        if (plansChanged) {
            debug("plans.json changed in last 30 minutes");
            initOptions.syncConsumers = true;
        }

        const apisPath = path.join(rootFolder, 'apis');
        const apiFolders = fs.readdirSync(apisPath);
        for (let i = 0; i < apiFolders.length; i++) {
            const folder = apiFolders[i];
            const configPath = path.join(apisPath, folder, 'config.json');
            if (fs.existsSync(configPath)) {
                const configChanged = isFileChanged(configPath, currentTime, 10);
                if (configChanged) {
                    changedApis.push(folder);
                }
            }
        }

        if (changedApis.length > 0) {
            initOptions.syncApis = true;
            initOptions.apisList = changedApis;
        }
        debug(changedApis);
        debug("done with getChangedFolders");
    }
    catch(err) {
        debug('error occured during the api changed files detection')
        debug(err)
    }
}