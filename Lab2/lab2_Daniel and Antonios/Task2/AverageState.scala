package sparkstreaming

case class AverageState() {
    var count: Long = 0
    var sum: Long = 0

    def average(): Double = {
        if (count < 1) {
            throw new Exception("Count is less than 1 when computing average")
        }
        count / sum
    }

    def update(entry: Int) = {
        count += 1
        sum += entry
    }
}