const bq = require('gcloud')({ projectId: process.env.BQ_PROJECT_ID }).bigquery()
const datasetNames = [
    'views',
    'users',
    'lyric_viwes',
]

exports.sendEvent2BQ = (req, res) => {
    // TODO: web-frontとapiだけ許可する
    res.header('Access-Control-Allow-Origin', '*')
    res.header('Access-Control-Allow-Credentials', true)
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Origin, X-Requested-With, Accept')
    res.header('Access-Control-Allow-Methods', 'POST,OPTIONS')

    // 一旦外す
    // Lib.auth(req, res)

    let eventType = req.body.type
    if(eventType == "") {
      	res.status = 400
        res.end("イベントタイプが設定されていません。")
    }

    if(eventType == null) {
        res.status = 400
        res.end("イベントタイプがnullです。")
    }

    let ds = null
    datasetNames.map(name => {
        if(eventType == name) {
            ds = bq.dataset(name)
        }
    })

    if(!ds) {
      	res.status = 400
        res.end(`イベントタイプ'${eventType}'は不正です。`)
    }

    const tableName = TableUtil.getCurrentMonthTableName(eventType)
    const targetTable = ds.table(tableName)

    targetTable.exists((err, exists) => {
        let insertData = JSON.parse(req.body.data)
        const cb = () => {
            TableOperator.insert2bq(targetTable, insertData, res)
        }

        if(!exists) {
            TableOperator.createTable(ds, res, eventType, cb)
        } else {
            cb()
        }

    })

}

class TableOperator {
    static schemes() {
        // 全てのテーブルで共通するカラム
        const commonParams = [
            {
                "type": "timestamp",
                "mode": "required",
                "name": "createdAt",
            },
            {
                "type": "string",
                "mode": "required",
                "name": "id",
            },
            {
                "type": "string",
                "name": "sessionId",
            },
            {
                "type": "string",
                "name": "browser",
            },
            {
                "type": "string",
                "name": "os",
            },
            {
                "type": "string",
                "name": "city",
            },
            {
                "type": "string",
                "name": "referrer",
            },
            {
                "type": "string",
                "name": "initialReferrer",
            },
            {
                "type": "string",
                "mode": "required",
                "name": "currentUrl",
            },
        ]

        return {
            views: {
                schema: {
                    "fields": [
                        {
                            "type": "string",
                            "name": "userID",
                        },
                    ].concat(this.commonParams),
                },
            },
            users: {
                schema: {
                    "fields": [
                        {
                            "type": "string",
                            "name": "oauthID",
                        },
                    ].concat(this.commonParams),
                },
            },
            lyric_views: {
                schema: {
                    "fields": [
                        {
                            "type": "string",
                            "name": "userID",
                        },
                        {
                            "type": "string",
                            "name": "lyricID",
                        },
                    ].concat(this.commonParams),
                },
            },
        }
    }

    static createTable(ds, res, tablePrefix, cb) {
        const tableScheme = this.getScheme(tablePrefix)
        const tableName = TableUtil.getCurrentMonthTableName(tablePrefix)
        if(!tableScheme) {
          	res.status = 400
            res.end(`Table scheme was not found by name: ${tablePrefix}`)
            return
        }

        ds.createTable(tableName, tableScheme, (err, table, apiResponse) => {
            if ( err ) {
                console.error('err: ', err)
                console.error('apiResponse: ', apiResponse)
                res.status = 500
              	res.end("TABLE CREATION FAILED:" + JSON.stringify(err))
                return
            } else {
	            console.log("TABLE CREATED")
    	        cb()
            }
        })
    }

    // keyを元にテーブルスキーマを取ってくる
    // memo: 本当はキーに変数を指定するのはよくない。
    static getScheme(name) {
        const scm = this.schemes()
        if(scm[name]) {
            return scm[name]
        }
        return null
    }

    static insert2bq(table, insertData, res) {
        let data = {
                insertId: (new Date()).getTime(),
                json: insertData
            },
            options = {
                raw: true,
                skipInvalidRows: true,
            }

        table.insert(data, options, (err, insertErrors, apiResponse) => {
            if (err) {
                console.log('err: ', err)
                console.log('insertErr: ', insertErrors)
                console.log('apiResponse: ', JSON.stringify(apiResponse))
                res.status = 500
              	res.end("FAILED:" + JSON.stringify(err) + JSON.stringify(insertErrors))
            } else {
                res.status = 200
	            res.end("SUCCEED:" + JSON.stringify(apiResponse))
            }
        })
    }

    // リクエストデータとテーブルスキーマが合致するか検証
    static isValidScheme(name, data) {
        const scm = this.schemes(name)
        // ...WIP...
    }

}

/*
 * テーブルutility
 */
class TableUtil {
    static getPostfix() {
        const d = new Date()
        let m = d.getMonth() + 1
        m = (m < 10) ? "0" + m : m
        return `${d.getFullYear()}${m}`
    }

    static getCurrentMonthTableName(name) {
        return `${name}_${this.getPostfix()}`
    }
}

/*
 * アプリ共通lib
 */
class Lib {
    auth(req, res) {
        const authorization = req.get('Authorization')
        if(Util.decodeBase64(authorization) != process.env.AUTH_TOKEN) {
            res.status = 401
              res.send('INVALID AUTH TOKEN.')
        }
    }
}

/*
 * アプリ共通utility
 */
class Util {    
    decodeBase64(s) {
        return new Buffer(s || "", 'base64')
    }    
}