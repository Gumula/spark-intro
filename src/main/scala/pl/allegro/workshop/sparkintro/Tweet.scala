package pl.allegro.workshop.sparkintro

case class UserMention(
                        id: Long,
                        id_str: String,
                        name: String,
                        indices: List[Long],
                        screen_name: String
                      )

case class Entity(
                   hashtags: List[Hashtag],
                   user_mentions: List[UserMention],
                   urls: List[Url]
                 )

case class User(
                 id: Long,
                 id_str: String
               )

case class RetweetedStatus(
                            id: Long,
                            favorited: Boolean,
                            is_quote_status: Boolean,
                            text: String,
                            source: String,
                            truncated: Boolean,
                            created_at: String,
                            user: User,
                            geo: String,
                            in_reply_to_user_id_str: String,
                            coordinates: String,
                            in_reply_to_user_id: String,
                            possibly_sensitive: Option[Boolean],
                            contributors: String,
                            //place: String,
                            in_reply_to_status_id_str: String,
                            retweeted: Boolean,
                            lang: String,
                            favorite_count: Long,
                            in_reply_to_screen_name: String,
                            in_reply_to_status_id: String,
                            id_str: String,
                            retweet_count: Long,
                            entities: Entity
                          )

case class Tweet(
                  id: Long,
                  entities: Entity,
                  is_quote_status: Boolean,
                  text: String,
                  favorited: Boolean,
                  source: String,
                  truncated: Boolean,
                  created_at: String,
                  user: User,
                  geo: Geo,
                  in_reply_to_user_id_str: String,
                  coordinates: Geo,
                  in_reply_to_user_id: String,
                  contributors: String,
                  //place: String,
                  in_reply_to_status_id_str: String,
                  retweeted: Boolean,
                  lang: String,
                  favorite_count: Long,
                  in_reply_to_screen_name: String,
                  in_reply_to_status_id: String,
                  retweeted_status: RetweetedStatus,
                  id_str: String,
                  retweet_count: Long
                )

case class Hashtag(
                    indices: List[Long],
                    text: String
                  )

case class Url(
                indices: List[Long],
                display_url: String,
                expanded_url: String,
                url: String
              )

case class Geo(
             coordinates: Array[Double],
             `type`: String
             )

