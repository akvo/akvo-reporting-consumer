create table survey (
  id bigint primary key,
  display_text text,
  description text,
  folder_id bigint,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp
);

create index on survey (folder_id);

create table folder (
  id bigint primary key,
  display_text text,
  parent_id bigint,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp
);

create index on folder (parent_id);

create table form (
  id bigint primary key,
  survey_id bigint not null,
  display_text text,
  description text,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp
);

create index on form (survey_id);

create table data_point (
  id bigint primary key,
  survey_id bigint not null,
  identifier text,
  latitude double precision,
  longitude double precision,
  elevation double precision,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp
);

create index on data_point (survey_id);

create table form_instance (
  id bigint primary key,
  form_id bigint not null,
  data_point_id bigint not null,
  submitter text,
  submitted_at timestamp,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp
);

create index on form_instance (form_id);
create index on form_instance (data_point_id);

create table question_group (
  id bigint primary key,
  form_id bigint not null,
  display_order int,
  repeatable boolean default false,
  display_text text,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp,
  unique (form_id, display_order)
);

create index on question_group (form_id);

create table question (
  id bigint primary key,
  question_group_id bigint not null,
  type text,
  display_order int,
  display_text text,
  identifier text,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp,
  unique (question_group_id, display_order)
);

create index on question (question_group_id);

create table response (
  id bigint primary key,
  form_instance_id bigint not null,
  question_id bigint not null,
  value jsonb not null,
  iteration int default 0,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp,
  unique (form_instance_id, question_id, iteration)
);

create index on response (form_instance_id);
create index on response (question_id);

create table consumer_offset (
  "offset" bigint not null
);

insert into consumer_offset values (0);
