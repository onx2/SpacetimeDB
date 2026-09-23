//! The SQL that typed queries from generated bindings turn into. No network; `live.rs` subscribes
//! with one.

#[allow(dead_code)]
mod chat_bindings;
#[allow(dead_code)]
mod view_bindings;

use spacetimedb_bevy::TypedQuery;

#[test]
fn a_table_a_filter_and_a_join() {
    use chat_bindings::query;
    assert_eq!(query::user().into_sql(), r#"SELECT * FROM "user""#);
    assert_eq!(
        query::user().filter(|user| user.online.eq(true)).into_sql(),
        r#"SELECT * FROM "user" WHERE ("user"."online" = TRUE)"#
    );
    assert_eq!(
        query::message()
            .filter(|message| message.text.ne(String::new()).and(message.text.ne("spam".to_owned())))
            .into_sql(),
        r#"SELECT * FROM "message" WHERE (("message"."text" <> '') AND ("message"."text" <> 'spam'))"#
    );

    // Joins go through columns that have an index, which is all `ix_cols` offers.
    use view_bindings::query as views;
    let levels_of_players = views::player_level()
        .left_semijoin(views::player(), |level, player| level.entity_id.eq(player.entity_id))
        .into_sql();
    assert_eq!(
        levels_of_players,
        r#"SELECT "player_level".* FROM "player_level" JOIN "player" ON "player_level"."entity_id" = "player"."entity_id""#
    );
}
