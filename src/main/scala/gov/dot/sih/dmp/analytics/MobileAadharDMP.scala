package gov.dot.sih.dmp.analytics

/**
  * Created by imuthusamy on 31/03/18.
  */
class MobileAadharDMP {}

case class AadharMobileDataRow(aadhar: String,
                               name: String,
                               mobileNumber: String,
                               operator: String,
                               lsa: String,
                               doR: String,
                               ipAddress: String,
                               dropStatus: String) {

  def printRecord() = {

    val row =
      s"${aadhar}|${name}|${mobileNumber}|${operator}|${lsa}|${doR}|${ipAddress}|${dropStatus}"

    row
  }
}

case class NumberLong($numberLong: String)

case class MobileDetail(_id: String,
                        number: NumberLong,
                        operator: String,
                        region: String,
                        dateOfReg: String,
                        ipAddress: String,
                        dropStatus: Boolean)

case class AadharMobileData(_id: String,
                            aadharNumber: NumberLong,
                            name: String,
                            mobileDetails: Array[MobileDetail],
                            lastLogin: Array[String],
                            __v: Int) {

  def printTheDataAsNeeded: String =
    s" ${this._id} ~ ${this.name} ~ ${this.mobileDetails.size}"
}

case class MongoMobileDetail(_id: String,
                             number: Long,
                             operator: String,
                             region: String,
                             dateOfReg: String,
                             ipAddress: String,
                             dropStatus: Boolean)

case class MongoAadharMobileData(_id: String,
                                 aadharNumber: Long,
                                 name: String,
                                 mobileDetails: Array[MongoMobileDetail],
                                 lastLogin: Array[String],
                                 __v: Int) {

  def printTheDataAsNeeded: String =
    s" ${this._id} ~ ${this.name} ~ ${this.mobileDetails.size}"
}
