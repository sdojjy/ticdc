use validator_basic;
insert into t1 values
    (2, 'b', now(), '2022-04-29 15:30:00', 0.123456789123456789, 1234567891234561234567891.23456789),
    (3, 'c', now(), '2022-04-29 15:30:00', 123456789123.456789, 123456789123456123.456789123456789),
    (4, 'd', now(), '2022-04-29 15:30:00', 1234567.89123456789, 12345678912.3456123456789123456789),
    (5, 'e', now(), '2022-04-29 15:30:00', 12.3456789123456789, 0.123456789123456123456789123456789),
    (6, 'f', now(), '2022-04-29 15:30:00', 123456789123456789, 123456789123456123456789123456789),
    (7, 'g', now(), '2022-04-29 15:30:00', 0.123456789123456789, 1234567891234561234567891.23456789);
delete from t1 where name='a'; -- id=1
update t1 set name='bb' where id = 2;
-- trigger syncer flush checkpoint
alter table t1 comment 'a';