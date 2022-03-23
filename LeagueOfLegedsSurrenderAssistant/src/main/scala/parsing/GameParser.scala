package parseGame

import models.{RawGameState, GameState}

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}

object GameParser {
    def parseGameWithLabels(spark: SparkSession, games: DataFrame): Dataset[GameState] = {
        import spark.implicits._
        val rawGameState = games.as[RawGameState]
        rawGameState.map(parseGame)
    }

    def parseGame(game: RawGameState): (GameState) = {
        GameState(
            game.matchId,
            game.blue_win,
            game.blueAvgLevel - game.redAvgLevel,
            game.blueGold - game.redGold,
            game.blueMinionsKilled - game.redMinionsKilled,
            game.blueJungleMinionsKilled - game.redJungleMinionsKilled,
            game.blueTowersDestroyed,
            game.redTowersDestroyed,
            game.blueHeraldKills,
            game.redHeraldKills,
            game.blueDragonKills,
            game.redDragonKills)
    }
}