package models

case class GameState(
    val matchId: Long,
    val blueWin: Int,
    val deltaAverageLevel: Double,
    val deltaGold: Int,
    val deltaMinionsKilled: Int,
    val deltaJungleMinionsKilled: Int,
    val blueTowersDestroyed: Int,
    val redTowersDestroyed: Int,
    val blueHeraldKills: Int,
    val redHeraldKills: Int,
    val blueDragonKills: Int,
    val redDragonKills: Int)