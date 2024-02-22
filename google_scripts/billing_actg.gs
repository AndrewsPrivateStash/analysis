/*
  generate the monthly accounting data needed for billing
  drop off the CSV on the 3rd of each month
  and email acctg the link and summary
  
*/

function runBillingReport() {
  const curDate = new Date();
  const rptDate = new Date(curDate.getFullYear(), curDate.getMonth()-1, 1);
  const dateStamp = Utilities.formatDate(curDate, "America/Los_Angeles", "yyyyMMdd");
  const rptStamp = Utilities.formatDate(rptDate, "America/Los_Angeles", "MMMyyyy");
  const rptPrettyStamp = Utilities.formatDate(rptDate, "America/Los_Angeles", "MMMMM yyyy");

  const destFolder = DriveApp.getFolderById("14sA-rFWZBG_PnfnTWa95dOMn7TKtSHhX");
  const queries = [
    {
      "label": "detail_data",
      "query": "select * from ops.actg_billing_data;",
      "filename": rptStamp + "_actg_activity_" + dateStamp + ".csv",
      "dest": destFolder,
      "title": null
    },
    {
      "label": "general_totals",
      "query": "select * from ops.actg_billing_totals;",
      "filename": null,
      "dest": null,
      "title": "Reward Totals"
    },
    {
      "label": "tab_detail",
      "query": "select * from tab.acct_monthly_billing_support;",
      "filename": rptStamp + "_tab_support_" + dateStamp + ".csv",
      "dest": destFolder,
      "title": null
    },
    {
      "label": "tab_totals",
      "query": "select * from tab.acct_monthly_billing;",
      "filename": null,
      "dest": null,
      "title": "TAB Billing Totals"
    }
  ];

  const rec = ["somebody@bumped.com"];
  const ccs = ["andrew.pfaendler@bumped.com", "another.guy@bumped.com"];
  //const rec = ["andrew.pfaendler@bumped.com"];
  //const ccs = ["apfaendler@gmail.com"];
  const subj = rptPrettyStamp + " Billing Data";
  
  const queryResults = collectData(queries);

  // check if results exist
  let noDataFlag = false;
  let datArr =[];
  if (queryResults.size == 0) {
    Logger.log("no data back from queries!");
    noDataFlag = true;
  } else {
    for (const val of queryResults.values()) {
      if (val.title) { // only grab data with titles
        datArr.push(
          {
            "dat": val.results,
            "title": val.title
          }
        )
      }
    }
  }

  // construct the email
  Logger.log("generating email ..");

  // look for empty data case
  if (noDataFlag) {
    const emptyBody = "no data was found, nothing to bill!?..";
    MailApp.sendEmail(rec.join(','), subj, emptyBody, {
      name: 'Bumped Inc. Reports',
      cc: ccs.join(',')
    });
  } else {
      const body = "whoops, HTML tables failed for some reason!.."
      // collect files
      let filesToAttach = Array.from(queryResults.values()).filter(v => Object.keys(v.file).length > 0).map(v => v.file.getAs("text/csv"));
      MailApp.sendEmail(rec.join(','), subj, body, {
        name: 'Bumped Inc. Reports',
        cc: ccs.join(','),
        attachments: filesToAttach,
        htmlBody: buildEmailHTML(datArr, 100, 1000, destFolder.getUrl())
      });
  }
}


function collectData(queries) {
  // take an array of objects [{label<String>, query<String>, filename<String>, dest<FolderObj>, title<String>}]
  // return a map {label, {results<arr>, fileID, title<String>}}
  const results = new Map();
  for (const q of queries) {
    Logger.log("querying " + q.label);
    let qRes = [];
    let fileObj = {};

    const qRawRes = dataPull(q.query, true);  // raw string result
    if (!qRes) {
      Logger.log(q.label + " query returned no results, skipping");
      continue;
    } 
    qRes = qRawRes.split("\n").map(line => line.split(','));  // convert to array

    if (q.filename) {
      const f = makeCSV(q.filename, qRawRes, q.dest);
      fileObj = DriveApp.getFileById(f);
    }
    
    results.set(q.label, {"results": qRes, "file": fileObj, "title": q.title})
  }
  return results;
}



function buildEmailHTML(arrDat, mult, maxWidth, urlPath) {
  // construct various HTML bits
  // arrDat is array of objects {dat, title}
 
  let htmlString = "";
  // build tables
  for (const d of arrDat) {
    htmlString += buildHTMLTable(d.dat, d.title, mult, maxWidth);
  }

  // closing
  htmlString += "<p><br>Detail attached, and all detail files can be found here: </p>";
  htmlString += urlPath;

  return htmlString;
}

function buildHTMLTable(dat, title, mult, maxWidth) {
  // assume 100px per column up to max of 1k
  // returns a string segment for the weekly totals table enclosed in <br> line breaks
  const tabelWidth = Math.min(dat[0].length * mult, maxWidth);
  let htmlString = "<br>";
  htmlString += "<h4>"+ title + "</h4>" + // title of table
                '<table width="' + tabelWidth + '" border="1">';

  dat.forEach(row => htmlString += buildHTMLTableRow(row));

  htmlString += "</table>";
  //htmlString += "<br>";
  return htmlString;
}

function buildHTMLTableRow(vec) {
  let htmlString = "<tr>";
  vec.forEach(val => htmlString += "<th>" + val + "</th>");
  htmlString += "</tr>";
  return htmlString;
}


/* ref
      htmlBody: "<h4>Totals</h4>" +
                '<table width="640" border="1">' +
                "  <tr>" +
                "    <th>" + totArr[0][0] + "</th>" +
                "    <th>" + totArr[0][1] + "</th>" +
                "    <th>" + totArr[0][2] + "</th>" +
                "    <th>" + totArr[0][3] + "</th>" +
                "    <th>" + totArr[0][4] + "</th>" +
                "    <th>" + totArr[0][5] + "</th>" +
                "  </tr>" +
                "  <tr>" +
                "    <td>" + totArr[1][0] + "</td>" +
                "    <td>" + totArr[1][1] + "</td>" +
                "    <td>" + totArr[1][2] + "</td>" +
                "    <td>" + totArr[1][3] + "</td>" +
                "    <td>" + totArr[1][4] + "</td>" +
                "    <td>" + totArr[1][5] + "</td>" +
                "  </tr>" +
                "</table>" +
                "<p><br>Detail attached, and all detail files can be found here: </p>" +
*/                
