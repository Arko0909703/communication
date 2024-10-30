require("dotenv").config();
const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const path = require('path');
const http = require('http');
const fs = require('fs');
const multer = require('multer');
//const csv = require('fast-csv');
const hbs = require('hbs');
const csvParser = require('csv-parser');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const snowflake = require('snowflake-sdk');
const app=express();
const mysql = require('mysql');
var moment = require('moment');
const { channel } = require("diagnostics_channel");
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true,
  }),
);

/*const mysql_connection = mysql.createConnection({
  //host : '3.108.92.87',
  host  : '30.0.0.49',
  user : 'kfcmyorderapp',
  password : 'frog00gOT!',
  database  : 'KFCIndiaProd'
});

mysql_connection.connect((err, conn)=>{

  if(err){
    console.error('Undable to connect: ' + err.message)
  }else{
    console.log('Successfully connected mysql')
  }

})*/


// Snowflake connection options

const connectionOptions = {
  account: 'mo35201.ap-south-1',
  username: 'REPORTING',
  password: 'Kfc@#123',
  warehouse: 'COMPUTE_WH',
  database: 'POS',
  authenticator: 'SNOWFLAKE',
  clientSessionKeepAlive: true,
  application : 'ap-south-1.aws.snowflakecomputing.com',
  //schema: 'https'
};
const connection = snowflake.createConnection(connectionOptions);

connection.connect((err, conn) => {
  if (err) {
    console.log('Unable to connect: ' + err.message);
    res.json({ message: err });
  } else {
    console.log('Successfully connected to Snowflake');
  }
});

const saveOrderInfo = async function(transactionid,ordertype,processchannel,req, response){
  
  //console.log(JSON.stringify(req.body))
  checknum=''
  if(processchannel.toLowerCase()=='instore'){
  mobile=req.body.participants[0].identifyBy.identifier
  storename=req.body.participants[0].person.customAttributes.storename
  //mobile=req.body.participants[0].person.contactInformation.phone[0].number
  
  message_type=req.body.participants[0].person.customAttributes.msg_type

  if(req.body.participants[0].person.customAttributes.msg_type=='order_confirm_di_ta_tezty_express'){
    url=""
    checknum=req.body.participants[0].person.customAttributes.checknum
    ordertype=req.body.participants[0].person.customAttributes.order_type
  }
  else{
    ordertype=""
    url=""
  }
}
else
{
  storename=req.body.personData.customAttributes.storename
  mobile=req.body.personData.contactInformation.phone[0].number
  message_type=req.body.personData.customAttributes.msg_type
  if(req.body.personData.customAttributes.msg_type=='order_confirm_di_ta_tezty_express'){
    url=req.body.personData.customAttributes.invoice_url
    checknum=req.body.personData.customAttributes.checknum
    
  }
  else{
    url=req.body.personData.customAttributes.pickupreceipt
  }
}
  ordertype_name=ordertype
  processchannel_name=processchannel
  if(ordertype.toLowerCase()=='ta'||ordertype.toLowerCase()=='pickup-'||ordertype.toLowerCase()=='pickup'){
    ordertype='2';
  }
  else if(ordertype.toLowerCase()=='di'||ordertype.toLowerCase()=='dinein-'||ordertype.toLowerCase()=='dinein'){
    ordertype='1';
  }
  else if(ordertype.toLowerCase()=='dt'||ordertype.toLowerCase()=='pickedup'||ordertype.toLowerCase()=='dy'){
    ordertype='3';
  }

  if(processchannel.toLowerCase()=='instore'){
    processchannel='6'
  }
  else if(processchannel.toLowerCase()=='kiosk'){
    processchannel='4'
  }
  const sql_q = "insert into MACROMATIX_RDS.LAMBDA.lambda_order_log(champs_id,checknum,message,mobile_no,channel,franchise,check_url,message_type,message_delivery_date,order_type,channel_name,ordertype_name,transaction_id) values ('"+storename+"','"+checknum+"','"+response+"','"+mobile+"','" + processchannel + "','dil','"+url+"','"+message_type+"','"+moment().utcOffset("+05:30").format('YYYY-MM-DD HH:mm:ss')+"','"+ordertype+"','"+processchannel_name +"','"+ordertype_name+"','"+transactionid+"')";
  
  var statement = connection.execute({
    sqlText: sql_q,
    complete: function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
      } else {
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    }
  });

}

const updateGesInfo = async function(ids){
  const sql_q = "update MACROMATIX_RDS.LAMBDA.lambda_order_log set GES_SENT='y' where ID in ("+ids+")";
  var statement = connection.execute({
    sqlText: sql_q,
    complete: function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
      } else {
        console.log('Successfully executed statement: ' + stmt.getSqlText());
      }
    }
  });
  
}
const saveGesBulkOrderInfo=async function(payload,message,noofrequests,next){
  const sql_q = "insert into MACROMATIX_RDS.LAMBDA.LAMBDA_GES_BULK(payload,message,franchise,message_delivery_date,no_of_requests) values ('"+payload+"','"+message+"','dil','"+moment().utcOffset("+05:30").format('YYYY-MM-DD HH:mm:ss')+"',"+noofrequests+")";

  var statement = connection.execute({
    sqlText: sql_q,
    complete: function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
        next('failure')
      } else {
        console.log('Successfully executed statement: ' + stmt.getSqlText());
        next('sucess')
      }
    }
  });
  
}
const saveGesOrderInfo = async function(check_num,champs_id,data, message,mobile_no,channel,franchise,channel_name,ordertype_name,next){
  const sql_q = "insert into MACROMATIX_RDS.LAMBDA.LAMBDA_GES_MSG(champs_id,check_num,message,mobile_no,channel,franchise,body,message_delivery_date,channel_name,ordertype_name) values ('"+champs_id+"','"+check_num+"','"+message+"','"+mobile_no+"','" + channel + "','dil','"+data+"','"+moment().utcOffset("+05:30").format('YYYY-MM-DD HH:mm:ss')+"','"+channel_name+"','"+ordertype_name+"')";

  var statement = connection.execute({
    sqlText: sql_q,
    complete: function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
        next('failure')
      } else {
        console.log('Successfully executed statement: ' + stmt.getSqlText());
        next('sucess')
      }
    }
  });
  
}


app.get('/api/v1/dil/gesmessage', (req, res) => {
  if(req.headers.authorization != process.env.AUTH_KEY){
    res.status(403).json({ err_msg: 'Invalid AUTH token Id' });
    return;
  } 
  
  connection.execute({
    sqlText: "SELECT top 500 ID, CONCAT('https://s.kfcvisit.com/IND?CN=',champs_id,TO_VARCHAR(message_delivery_date, 'ddmmyyyyhhmi'),'&V=',order_type,'&O=',CHANNEL,'&email=&EQ=3&Source=Email&FN=&LN=&Phone=',mobile_no) AS url,champs_id,checknum,mobile_no,message_delivery_date AS order_date,channel,channel_name,ordertype_name FROM MACROMATIX_RDS.LAMBDA.lambda_order_log WHERE message_type='order_confirm_di_ta_tezty_express' AND ges_sent IS NULL AND order_type IS NOT NULL and franchise='dil' and TIMESTAMPDIFF(SECOND,message_delivery_date,cast(convert_timezone('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS datetime))>=1800 and order_type not like '%Cancelled%'",
    complete: async function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
      } else {
        console.log(this.sqlText)
        console.log('Number of rows produced: ' + rows.length);
        ids="'0'";
        const promises = []; 
        let gesObjects=[];
        for(var i=0;i<rows.length;i++){
          id=rows[i].ID;
          mobile_no=rows[i].MOBILE_NO;
          champs_id=rows[i].CHAMPS_ID;
          url=rows[i].URL;
          check_num=rows[i].CHECKNUM;
          processchannel=rows[i].CHANNEL;
          channel_name=rows[i].CHANNEL_NAME;
          ordertype_name=rows[i].ORDERTYPE_NAME;
          gesObjects.push(prepareGesPayload(mobile_no,champs_id,url,check_num,processchannel,channel_name,ordertype_name));
          ids=ids+",'"+id+"'";
        }
        promises.push(sendgesmessage(gesObjects,0))
        Promise.all(promises)
        .then(results => {
          console.log("All promises resolved:", results);
          saveGesBulkOrderInfo(JSON.stringify(gesObjects),results,gesObjects.length,function(){
              
          });
          
          res.json({ message: 'GES Message triggered successfully' });
        })
        .catch(error => {
          console.error("One or more promises rejected:", error);
        });
        await updateGesInfo(ids);
        //res.json({ message: 'GES Message triggered successfully' });
      }
    }
  });
  //res.json({ message: 'GES Message triggered successfully' });
});

app.get('/api/v1/dil/qualtricksgesmessage', (req, res) => {
  if(req.headers.authorization != process.env.AUTH_KEY){
    res.status(403).json({ err_msg: 'Invalid AUTH token Id' });
    return;
  } 
  
  connection.execute({
    sqlText: "SELECT TOP 500 ID, concat('https://customer.kfc-listens.com/jfe/form/SV_8bHC0noyvM3jPWC?Q_EED=',BASE64_ENCODE(concat('{\"S\":\"',champs_id,'\",\"OI\":\"',checknum,'\",\"D\":\"',TO_VARCHAR(message_delivery_date, 'YYYY-MM-DD'),'\",\"TM\":\"',concat(TO_VARCHAR(message_delivery_date, 'HH:MI:ss'),'.000'),'\",\"TI\":\"',checknum,'\",\"OT\":\"',order_type,'\",\"SM\":\"',channel,'.0\",\"PH\":\"',mobile_no,'\"}'))) AS url,champs_id,checknum,mobile_no,message_delivery_date AS order_date,channel FROM MACROMATIX_RDS.LAMBDA.lambda_order_log  WHERE message_type='order_confirm_di_ta_tezty_express' AND ges_sent IS NULL AND order_type IS NOT NULL and franchise='dil' and TIMESTAMPDIFF(SECOND,message_delivery_date,cast(convert_timezone('Asia/Kolkata', CURRENT_TIMESTAMP(2)) AS datetime))>=3600 and order_type not like '%Cancelled%' and mobile_no not in('919999999999', '918888888888', '917777777777', '916666666666') and length(mobile_no)=12 and substr(mobile_no,3,1) in ('9','8','7','6')",
    complete: async function(err, stmt, rows) {
      if (err) {
        console.error('Failed to execute statement due to the following error: ' + err.message);
      } else {
        console.log(this.sqlText)
        console.log('Number of rows produced: ' + rows.length);
        ids="'0'";
        const promises = []; 
        let gesObjects=[];
        for(var i=0;i<rows.length;i++){
          id=rows[i].ID;
          mobile_no=rows[i].MOBILE_NO;
          champs_id=rows[i].CHAMPS_ID;
          url=rows[i].URL;
          check_num=rows[i].CHECKNUM;
          processchannel=rows[i].CHANNEL;
          channel_name=rows[i].CHANNEL_NAME;
          ordertype_name=rows[i].ORDERTYPE_NAME;
          gesObjects.push(prepareGesPayload(mobile_no,champs_id,url,check_num,processchannel,channel_name,ordertype_name));
          ids=ids+",'"+id+"'";
        }
        promises.push(sendgesmessage(gesObjects,0))
        Promise.all(promises)
        .then(results => {
          console.log("All promises resolved:", results);
          saveGesBulkOrderInfo(JSON.stringify(gesObjects),results,gesObjects.length,function(){
              
          });
         
          
          res.json({ message: 'GES Message triggered successfully' });
        })
        .catch(error => {
          console.error("One or more promises rejected:", error);
        });
        await updateGesInfo(ids);
        //res.json({ message: 'GES Message triggered successfully' });
      }
    }
  });
  //res.json({ message: 'GES Message triggered successfully' });
});

function prepareGesPayload(mobile,champs_id,url,check_num,channel,channel_name,ordertype_name) {
  const d = new Date();
  gesPayload={
            "identifyBy": {
                "identifier": mobile,
                "type": "PHONE"
            },
            "person": {
                "customAttributes": {
                    "msg_type": "takeaway_dinein_feedback",
                    "storename": champs_id,
                    "kfc_storename": champs_id,
                    "storetype": "dil",
                    "url": url, 
                    "day_of_week": d.getDay()+1,
                    "storeid": champs_id
                },
                "contactInformation": {
                    "phone": [
                        {
                            "number": mobile
                        }
                    ]
                }
            }
        };
     /* saveGesOrderInfo(check_num,champs_id,JSON.stringify(gesPayload),'Bulk upload',mobile,channel,'dil',channel_name,ordertype_name,function(){
          //resolve(JSON.stringify(response.body));
        });*/
  return gesPayload;
}

const sendgesmessage = async function(gesObjects,interval){
  return new Promise((resolve, reject) => {
  data={
    "participants": gesObjects
    };
var req = {
  url: 'https://api.infobip.com/moments/1/flows/200000022004571/participants?phone=',
  method: 'POST',
  headers: {
    'Authorization' : 'App 09ccbef35ab3e87097b1e78658f39261-8a3f6e47-768c-4ea0-a380-9b5d5873d2e3',
    'Content-Type': 'application/json'
  },
  body:  data,    
  json: true

};

  
  request(req, function(error, response, body){
  console.log('Data:',response.body.operationId);
  //const api_response = response.body ? response.body : 'Message Sent'
  resolve(JSON.stringify(response.body.operationId));

});
})


}

app.post('/api/v1/dil/kiosk/ordermessage', (req, res) => {
  if(req.headers.authorization != process.env.AUTH_KEY){
    res.status(403).json({ err_msg: 'Invalid AUTH token Id' });
    return;
  }        
        const d = new Date();  
        const reqdata=req;
        var data=''
        ordertype=''
        transaction_id=""
        mobile=req.query.phone
       /* if(ordertype.includes("Cancelled")){
          res.status(400).json({ err_msg: 'Cancelled Order' });
          return;
        }*/
        //console.log(JSON.stringify(req.body))
        if(req.body.personData.customAttributes.msg_type=='order_confirm_di_ta_tezty_express'){
          transaction_id=req.body.personData.customAttributes.transaction_id
          ordertype=req.body.personData.customAttributes.order_type
          //ordertype=req.body.personData.customAttributes.order_type
          data={
            "participants": [
                {
                    "identifyBy": {
                        "identifier": req.query.phone,
                        "type": "PHONE"
                    },
                    "person": {
                        "customAttributes": {
                            "msg_type": "order_confirm_di_ta_tezty_express",
                            "storename": req.body.personData.customAttributes.storename,
                            "kfc_storename": req.body.personData.customAttributes.kfc_storename,
                            "storetype": "dil",
                            "checknum": req.body.personData.customAttributes.checknum,
                            "invoice_url": req.body.personData.customAttributes.invoice_url,
                            "day_of_week": d.getDay()+1,
                            "storeid": req.body.personData.customAttributes.storename
                        },
                        "contactInformation": {
                            "phone": [
                                {
                                    "number": req.query.phone
                                }
                            ]
                        }
                    }
                }
            ]
        };
        }
        else{
          ordertype=""
          data={
            "participants": [
                {
                    "identifyBy": {
                        "identifier": req.query.phone,
                        "type": "PHONE"
                    },
                    "person": {
                        "customAttributes": {
                            "msg_type": req.body.personData.customAttributes.msg_type,
                            "kfc_storename": req.body.personData.customAttributes.kfc_storename,
                            "orderminutes": req.body.personData.customAttributes.orderminutes,
                            "pickupreceipt": req.body.personData.customAttributes.pickupreceipt,
                            "storename": req.body.personData.customAttributes.storename,
                            "storetype": "dil",
                            "day_of_week": d.getDay()+1,
                            "storeid": req.body.personData.customAttributes.storename
                        },
                        "contactInformation": {
                            "phone": [
                                {
                                    "number": req.query.phone
                                }
                            ]
                        }
                    }
                }
            ]
        }
        }
      

          var req = {
              url: 'https://api.infobip.com/moments/1/flows/200000023570638/participants?phone='+req.query.phone,
              method: 'POST',
              headers: {
                'Authorization' : 'App 09ccbef35ab3e87097b1e78658f39261-8a3f6e47-768c-4ea0-a380-9b5d5873d2e3',
                'Content-Type': 'application/json'
              },
              body:  data,    
              json: true

          };
          if(mobile.length !=12 || mobile=='919999999999'||mobile=='918888888888'||mobile=='917777777777'|| mobile=='916666666666'||mobile.substr(2,1)=='1'||mobile.substr(2,1)=='2'||mobile.substr(2,1)=='3'||mobile.substr(2,1)=='4'||mobile.substr(2,1)=='5'||mobile.substr(2,1)=='0'){
            
            saveOrderInfo(transaction_id,ordertype,'kiosk',reqdata,"Not Sent");
            res.json({ message: "Not Sent" });
          }
          else{
          request(req,function(error, response, body){
            const api_response = response.body ? response.body : 'Message Sent'
            res.json({ message: api_response });
            saveOrderInfo(transaction_id,ordertype,'kiosk',reqdata,response.body.operationId);
          });
        }
      }
  );


app.post('/api/v1/dil/ordermessage', (req, res) => {
  if(req.headers.authorization != process.env.AUTH_KEY){
    res.status(403).json({ err_msg: 'Invalid AUTH token Id' });
    return;
  }        
        const d = new Date();  
        const reqdata=req;
        var data=''
        console.log("mobile bumber " + req.query.phone)
        mobile=req.query.phone
        console.log(JSON.stringify(req.body))
        if (typeof mobile !== 'undefined') {
          
        }
        else{
           mobile=req.body.participants[0].identifyBy.identifier
           console.log("mobile bumber " +mobile)
        }
        if (typeof mobile !== 'undefined') {
          
        }
        else{
           mobile=req.body.participants[0].person.customAttributes.customer_mobile
           console.log("mobile bumber " +mobile)
        }
        if (typeof mobile !== 'undefined') {
          
        }
        else{
           mobile="919999999999"
           console.log("mobile bumber " +mobile)
        }
        //ordertype="DI"
        ordertype=req.body.participants[0].person.customAttributes.order_type
        req.body.participants[0].person.customAttributes.day_of_week= d.getDay()+1
        //delete req.body.participants[0].person.customAttributes.customer_mobile
        data=req.body

          var req = {
              url: 'https://api.infobip.com/moments/1/flows/200000022004571/participants?phone='+req.query.phone,
              method: 'POST',
              headers: {
                'Authorization' : 'App 09ccbef35ab3e87097b1e78658f39261-8a3f6e47-768c-4ea0-a380-9b5d5873d2e3',
                'Content-Type': 'application/json'
              },
              body:  data,    
              json: true

          };
          if(mobile.length !=12 || mobile=='919999999999'||mobile=='918888888888'||mobile=='917777777777'|| mobile=='916666666666'||mobile.substr(2,1)=='1'||mobile.substr(2,1)=='2'||mobile.substr(2,1)=='3'||mobile.substr(2,1)=='4'||mobile.substr(2,1)=='5'||mobile.substr(2,1)=='0'){
            
            saveOrderInfo('',ordertype,"InStore",req,"Not Sent");
            res.json({ message: "Not Sent" });
          }
          else{
          request(req,function(error, response, body){
            const api_response = response.body ? response.body : 'Message Sent'
            res.json({ message: api_response });
            saveOrderInfo('',ordertype,"InStore",req,response.body.operationId);
          });
        }
      }
  );




/*const port = process.env.PORT || 3000;
app.listen(port, () => {
   console.log(`Server is running on port ${port}`);
});*/

module.exports = app;