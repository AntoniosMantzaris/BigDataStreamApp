package models

case class GamePrediction(val matchid: Long, val bluewin: Int, val probability_prediction_blue_win: Double) {
    def toCassandraRow() = GamePredictionRow(matchid.toString(), bluewin.toString(), probability_prediction_blue_win.toString())
}

case class GamePredictionRow(val matchid: String, val bluewin: String, val probability_prediction_blue_win: String)