package models

case class RawGameState(
    val matchId: Long,
    val blue_win: Int,
    val blueGold: Int,
    val blueMinionsKilled: Int,
    val blueJungleMinionsKilled: Int,
    val blueAvgLevel: Double,
    val redGold: Int,
    val redMinionsKilled: Int,
    val redJungleMinionsKilled: Int,
    val redAvgLevel: Double,
    val blueChampKills: Int,
    val blueHeraldKills: Int,
    val blueDragonKills: Int,
    val blueTowersDestroyed: Int,
    val redChampKills: Int,
    val redHeraldKills: Int,
    val redDragonKills: Int,
    val redTowersDestroyed: Int
)