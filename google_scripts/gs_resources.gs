// #### Support Functions ####
// ###########################

function mailQuotaCheck(needed) {
  // returns true if quota remains for the needed number of emails
  let dq = MailApp.getRemainingDailyQuota();
  if(dq < needed) {
    Logger.log("not enough mail quota to email reports, only %d left.", dq);
    return false;
  }
  return true;
}

function mailFiles(fileIds, addrArr, ccs, subj, body) {
  let sendfiles = [];
  for (const f of fileIds) {
    let file = DriveApp.getFileById(f);
    sendfiles.push(file.getAs("text/csv"));
  }

  MailApp.sendEmail(addrArr.join(','), subj, body, {
    name: 'Bumped Inc. Reports',
    cc: ccs.join(','),
    attachments: sendfiles
  });
}

function clearFiles(folder, checkStr, metaStr, fileType) {
    // check files for checkstr and remove
    // can check txt, csv, or all
    Logger.log("checking for duplicate files containing: %s in %s", checkStr, metaStr);
    let fileCnt = 0;

    let mimeType, files;
    switch (fileType) {
      case "csv":
        mimeType = "text/csv";
        break;
      case "txt":
        mimeType = "text/plain";
        break;
      case "all":
        break;
      default:
        Logger.log("error bad mimetype: %s", fileType);
        throw new Error("mimetype");
    }

    if (mimeType) {
      files = folder.getFilesByType(mimeType);
    } else {
      files = folder.getFiles();
    }

    while(files.hasNext()) {
      let f = files.next();
      let nm = f.getName();
      if(nm.search(checkStr) != -1) {
        Logger.log("removing existing file: %s", nm);
        f.setTrashed(true);
        fileCnt++;
      }
    }
    if(fileCnt > 0) {
      Logger.log("removed %d files", fileCnt);
    }
}

function makeCSV(fileName, content, destination) {
    let newFile = DriveApp.createFile(fileName,content); //Create a new text file in the callers root folder
    newFile.moveTo(destination);
    return newFile.getId();
}
  
function query(proj, str) {
  let request = {
    query: str,
    useLegacySql: false
  };
  let queryResults = BigQuery.Jobs.query(request, proj);
  let jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  let sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(proj, jobId);
  }
  return queryResults;
}
  
function makeRows(queryResults, proj) {
  // Get all the rows of results.
  let jobId = queryResults.jobReference.jobId;
  let rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(proj, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }
  return rows;
}
  
function rollData(rows) {
  // store results in 2d array
  let data = new Array(rows.length);
  for (let i = 0; i < rows.length; i++) {
    let cols = rows[i].f;
    data[i] = new Array(cols.length);
    for (let j = 0; j < cols.length; j++) {
      data[i][j] = cols[j].v;
    }
  }  
  return data;
}
  
function dataPull(q, retHeader) {  
  const projectId = 'bumped-analytics-aw5325';

  let queryResults = query(projectId, q);
  let rows = makeRows(queryResults, projectId);
  
  if (rows) {
    let data = rollData(rows);
    let headers = queryResults.schema.fields.map(function(field) {
      return field.name;
    });
    
    let content = data.map(c => c.join()).join("\n"); // collapse array into string
    if (retHeader) {
      const hds = headers.join() + "\n";
      content = hds + content;
    }
    return content;
    
  } else {
    Logger.log('No rows returned.');
  }
}

function queryNoReturn(q) {
  const projectId = 'bumped-analytics-aw5325';
  Logger.log("running no-return query..")
  var queryResults = query(projectId, q);
  
  if (queryResults.jobComplete) {
    Logger.log('..success');
    return true
  } else {
    Logger.log('..failure!');
    return false
  }

}

/* random utils */
function make2dArray(str) {
  // make a 2d array from a string having
  // commas seperating columns and \n seperating rows
  return str.split("\n").map(line => line.split(','));
}

function makeMap(array2d) {
  // take 2d array (2xn) and return a map of key value pairs
  // where the first row is the labels and the second the values
  let outmap = new Map();
  for (let i = 0; i < array2d[0].length; i++) {
    outmap.set(array2d[0][i], array2d[1][i]);
  }
  return outmap;
}
