package org.sunbird.dp.core.denorm.`type`

import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.denorm.config.DenormalizationConfig
import org.sunbird.dp.core.denorm.domain.Event
import org.sunbird.dp.core.job.Metrics

class UserDenormalization(config: DenormalizationConfig) {

  private val userDataCache =
    new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config),
      config.userStore, config.userFields)
  userDataCache.init()

  def denormalize(event: Event, metrics: Metrics): Event = {
    val actorId = event.actorId()
    val actorType = event.actorType()
    if (null != actorId && actorId.nonEmpty && !"anonymous".equalsIgnoreCase(actorId) && "user".equalsIgnoreCase(actorType)) {
      metrics.incCounter(config.userTotal)
      val userData = userDataCache.getWithRetry(actorId)

      if (userData.isEmpty) {
        metrics.incCounter(config.userCacheMiss)
      } else {
        metrics.incCounter(config.userCacheHit)
      }
      if (!userData.contains("usersignintype"))
        userData.put("usersignintype", config.userSignInTypeDefault)
      if (!userData.contains("userlogintype"))
        userData.put("userlogintype", config.userLoginInTypeDefault)
      event.addUserData(userData)
    }
    event
  }

  def closeDataCache() = {
    userDataCache.close()
  }

}