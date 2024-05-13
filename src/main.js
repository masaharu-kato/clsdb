//  @ts-check
import * as MongoDB from "mongodb"
import { DB } from './clsdb.js'

class Category {
  /**
   * @param {number} no
   * @param {string} name
   */
  constructor(no, name) {
    this.no = no
    this.name = name
  }
}

class Items {
  /**
   * @param {Category} category 
   * @param {string} name
   * @param {number} price
   */
  constructor(category, name, price) {
    this.category = category
    this.name = name
    this.price = price
  }
}

async function main() {

  const mongo_client = new MongoDB.MongoClient('mongodb://localhost:27017')
  const mongo_db = mongo_client.db('data')
  const db = new DB(mongo_db)

  await mongo_db.dropDatabase()

  const cate1 = new Category(1, "cate1")
  const cate2 = new Category(2, "cate2")
  const categories = [cate1, cate2]

  const items = [
    new Items(cate1, "pen", 120),
    new Items(cate1, "eraser", 70),
    new Items(cate1, "note", 150),
    new Items(cate2, "item01", 140),
    new Items(cate2, "item02", 210),
    new Items(cate2, "item03", 170),
    new Items(cate2, "item04", 90),
  ]

  await db.set(categories)
  await db.set(items)

  const items_in_db = await db.collection(Items).find({})
  // console.log('cached:', db.collection(Items)._cached_docs)

  console.log(items_in_db.map(item => ({ ...item })))
  // console.log('items:', items_in_db)

  await mongo_client.close()

}

main()
