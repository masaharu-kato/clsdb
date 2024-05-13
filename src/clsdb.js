//  @ts-check
import * as MongoDB from "mongodb"

/** @typedef {MongoDB.BSON.ObjectId} ID */

export class DB {
  db
  /** @type {{[name: string]: DBCollection}} */
  _colls

  static _COLL_PROP = '__collection__'

  /**
   * @param {MongoDB.Db} db
   */
  constructor(db) {
    this.db = db
    this._colls = {}
  }

  /**
   * @param {any} arg
   */
  collection(arg) {
    const name = typeof arg === 'string' ? arg : arg.name
    if (!(name in this._colls)) {
      this._colls[name] = new DBCollection(this, this.db.collection(name))
    }
    return this._colls[name]
  }

  /**
   * @param {any[]} arg
   */
  async set(arg) {
    if (arg instanceof Array) {
      return await this.setMulti(arg)
    } else {
      return await this.setOne(arg)
    }
  }

  /**
   * @param {any} val
   */
  async setOne(val) {
    const collname = DB._getValCollname(val)
    if (!collname) {
      console.error('Invalid collname on value', val, collname)
      return
    }
    return await this.collection(collname).setOne(val)
  }

  /**
   * @param {any[]} vals
   */
  async setMulti(vals) {
    for await (const val of vals) {
      await this.setOne(val)
    }
  }

  // /**
  //  * @param {any[]} vals
  //  */
  // async setMulti(vals) {
  //   /** @type {{[collname: string]: Object[]}} */
  //   const inserts = {}
  //   /** @type {{[collname: string]: Object[]}} */
  //   const updates = {}

  //   for (const val of vals) {
  //     const collname = DB._getValCollname(val)
  //     if (!collname) {
  //       console.error('Invalid collname on value', val, collname)
  //       continue
  //     }
  //     const coll = this.collection(collname)
  //     const valobj = DB._getValObj(val)
  //     if (coll._isNewVal(valobj)) {
  //       (inserts[coll.name] ?? (inserts[coll.name] = [])).push(valobj)
  //     } else {
  //       (updates[coll.name] ?? (updates[coll.name] = [])).push(valobj)
  //     }
  //   }

  //   for await (const [collname, valobjs] of Object.entries(inserts)) {
  //     const coll = this._colls[collname]
  //     await coll.coll.insertMany(valobjs)
  //   }

  //   for await (const [collname, valobjs] of Object.entries(updates)) {
  //     const coll = this._colls[collname]
  //     for await (const valobj of valobjs) {
  //       await coll.coll.updateOne(coll._IdFromVal(valobj), valobj)
  //     }
  //   }

  // }

  /**
   * @param {any} val
   */
  static _getValCollname(val) {
    if (val instanceof Object && !(val instanceof Array)) {
      // console.log(val, val.constructor?.name)
      /** @type {string} */
      const collname = val.constructor?.name
      if (collname) return collname
    }
  }

  // /**
  //  * @param {any} val
  //  */
  // _getExistingValCollname(val) {
  //   const collname = DB._getValCollname(val)
  //   const coll = this._colls[collname]
  //   if (coll) return coll
  // }

  /**
   * @param {ID | string} _id
   */
  static _idToStr(_id) {
    if (typeof _id === 'string') return _id
    return _id.toHexString()
  }

  /**
   * @param {ID | string} idstr
   */
  static _strToId(idstr) {
    if (idstr instanceof MongoDB.ObjectId) return idstr
    return new MongoDB.ObjectId(idstr)
  }

  /**
   * @param {any} val
   */
  static _getValObj(val) {
    return Object.fromEntries(Object.keys(val).map(key => {
      const c_val = val[key]
      const collname = DB._getValCollname(c_val)
      return [key,
        collname ? {
          [DB._COLL_PROP]: collname,
          _id: c_val._id,
        } : c_val
      ]
    }))
  }

}

// const client = new MongoDB.MongoClient('mongodb://localhost:27017')
// const mongo_db = client.db('data')
// const coll = mongo_db.collection('hoge')

class DBCollection {

  /**
   * @param {DB} db
   * @param {MongoDB.Collection} coll
   */
  constructor(db, coll) {
    this.db = db
    this.coll = coll
    this.name = coll.collectionName
    this._cached_docs = {}
  }

  // async get(target, key) {
  //   // console.log('get:', target, key)
  //   this.coll.
  // },
  // async set(target, key, value) {
  //   console.log('set:', target, key, value)
  // },

  // /**
  //  * @param {ID | string} _id
  //  */
  // async hasCache(_id) {
  //   const idstr = DB._idToStr(_id)
  //   return idstr in this._cached_docs
  // }

  // /**
  //  * @param {ID | string} _id
  //  */
  // getCached(_id) {
  //   return this._cached_docs[DB._idToStr(_id)]
  // }

  /**
   * @param {ID} _id
   */
  async get(_id) {
    const fetcher = new DBDocsFetcher(this.db)
    const doc = this.prepareGet(_id, fetcher)
    await fetcher.fetch()
    return doc
  }

  /**
   * @param {ID} _id
   * @param {DBDocsFetcher} fetcher
   */
  prepareGet(_id, fetcher) {
    fetcher.addCollAndID(this, _id)
    return DBDocument.newWithProxy(this, _id)
  }

  /**
   * @param {ID[]} ids
   */
  async getMulti(ids) {
    const fetcher = new DBDocsFetcher(this.db)
    const doc = this.prepareGetMulti(ids, fetcher)
    await fetcher.fetch()
    return doc
  }

  /**
   * @param {ID[]} ids
   * @param {DBDocsFetcher} fetcher
   */
  prepareGetMulti(ids, fetcher) {
    return ids.map(_id => this.prepareGet(_id, fetcher))
  }

  /**
   * @param {MongoDB.Filter<MongoDB.BSON.Document>} filter
   */
  async find(filter) {
    const fetcher = new DBDocsFetcher(this.db)
    const doc = await this.prepareFind(filter, fetcher)
    await fetcher.fetch()
    return doc
  }

  /**
   * @param {MongoDB.Filter<MongoDB.BSON.Document>} filter 
   * @param {DBDocsFetcher} fetcher
   */
  async prepareFind(filter, fetcher) {
    const docs = await this.coll.find(filter).toArray()
    return docs.map(doc => this.prepareGet(doc._id, fetcher))
  }

  /**
   * @param {any} val
   */
  async setOne(val) {
    const valobj = DB._getValObj(val)
    if (this._isNewVal(valobj)) {
      const result = await this.coll.insertOne(valobj)
      val._id = result.insertedId
    } else {
      await this.coll.updateOne(this._IdFromVal(valobj), valobj)
    }
  }

  /**
   * @param {any} val
   */
  _isNewVal(val) {
    return !val._id
  }

  /**
   * @param {any} val
   */
  _IdFromVal(val) {
    return { _id: val._id }
  }

  // /**
  //  * @param {string} _id 
  //  */
  // _idFilter(_id) {
  //   return { _id }
  // }

}

class DBDocsFetcher {

  /**
   * @param {DB} db
   */
  constructor(db) {
    this.db = db
    /** @type {{[collname: string]: Set<string>}} */
    this._doc_ids = {}
  }

  /**
   * @param {Object} doc
   */
  addDoc(doc) {
    for (const [key, val] of Object.entries(doc)) {
      if (val instanceof Object && DB._COLL_PROP in val) {
        const c_coll = this.db.collection(val[DB._COLL_PROP])
        this.addCollAndID(c_coll, val._id)
      }
    }
  }

  /**
   * @param {DBCollection} coll
   * @param {ID | string} _id
   */
  addCollAndID(coll, _id) {
    const idstr = DB._idToStr(_id)
    // console.log('addCollAndID():', coll.name, idstr)
    if (!(idstr in coll._cached_docs)) {
      if (!(coll.name in this._doc_ids)) this._doc_ids[coll.name] = new Set()
      this._doc_ids[coll.name].add(idstr)
    }
  }

  async fetch() {
    // console.log('fetch():', this._doc_ids)
    if (!Object.keys(this._doc_ids).length) return
    const new_fetcher = new DBDocsFetcher(this.db)
    for await (const [collname, idset] of Object.entries(this._doc_ids)) {
      const coll = this.db._colls[collname]
      const docs = coll.coll.find({ _id: { $in: [...idset].map(DB._strToId) } })
      for await (const doc of docs) {
        if (!doc._id) throw new Error('doc._id cannot be null here.')
        coll._cached_docs[DB._idToStr(doc._id)] = doc
        new_fetcher.addDoc(doc)
      }
    }
    await new_fetcher.fetch()
    this._doc_ids = {}
  }

}

class DBDocument {

  /**
   * @param {DBCollection} coll
   * @param {ID} _id
   */
  constructor(coll, _id) {
    this.coll = coll
    this._id = _id
    this._rawdoc = undefined
  }

  /**
   * @param {DBCollection} coll
   * @param {ID} _id
   */
  static newWithProxy(coll, _id) {
    return new Proxy(new DBDocument(coll, _id), DBDocument._proxyHandler)
  }

  /**
   * @param {string | number | symbol} key
   */
  get(key) {
    const val = this.rawdoc[key]
    if (val instanceof Object && DB._COLL_PROP in val) {
      return this.coll.db.collection(val[DB._COLL_PROP])._cached_docs[DB._idToStr(val._id)]
    }
    return val
  }

  get rawdoc() {
    if (!this._rawdoc) {
      const rawdoc = this.coll._cached_docs[this._id]
      if (!rawdoc) throw new Error('Document is not fetched yet.')
      this._rawdoc = rawdoc
    }
    return this._rawdoc
  }

  get keys() {
    return Object.keys(this.rawdoc)
  }

  /** @type {ProxyHandler<DBDocument>} */
  static _proxyHandler = {
    get(target, key) {
      return target.get(key)
    },
    ownKeys(target) {
      return target.keys
    },
    getOwnPropertyDescriptor(target, prop) {
      return {
        enumerable: true,
        configurable: true,
      }
    }
  }

  // set(key, val) {
  // }

}

const db = new Proxy({}, {
  // async get(target, key) {
  //   console.log('get:', target, key)
  // },
  // async set(target, key, value) {
  //   console.log('set:', target, key, value)
  // },
  // deleteProperty(target, key)
  // ownKeys(target) {
  // has(target, key) {
  // defineProperty(target, key, descriptor) {
  // getOwnPropertyDescriptor(target, key) {
});

// console.log(proxy.hoge)
// proxy.fuga = 25
