CREATE DATABASE `busanalysis_dw` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `busanalysis_dw`;

CREATE TABLE `dim_bus_stop` (
  `id` int NOT NULL AUTO_INCREMENT,
  `legacy_id` int DEFAULT NULL,
  `name` varchar(500) DEFAULT NULL,
  `name_norm` varchar(500) GENERATED ALWAYS AS ((case when (regexp_substr(`name`,_utf8mb3'Terminal (Capão da Imbuia|Pinheirinho|Portão|Bairro Alto|Barreirinha|Boa Vista|Boqueirão|Cabral|Cachoeira|Caiuá|Campina do Siqueira|Campo Comprido|Capão Raso|Carmo|Centenário|CIC|Fazendinha|Hauer|Maracanã|Oficinas|Pinhais|Santa Cândida|Santa Felicidade|Sítio Cercado|Tatuquara|Guadalupe)') is null) then `name` else regexp_substr(`name`,_utf8mb3'Terminal (Capão da Imbuia|Pinheirinho|Portão|Bairro Alto|Barreirinha|Boa Vista|Boqueirão|Cabral|Cachoeira|Caiuá|Campina do Siqueira|Campo Comprido|Capão Raso|Carmo|Centenário|CIC|Fazendinha|Hauer|Maracanã|Oficinas|Pinhais|Santa Cândida|Santa Felicidade|Sítio Cercado|Tatuquara|Guadalupe)') end)) VIRTUAL,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `type` varchar(500) DEFAULT NULL,
  `type_norm` varchar(500) GENERATED ALWAYS AS ((case when ((`type` = _utf8mb3'Plataforma') or regexp_like(`name`,_utf8mb3'Terminal (Capão da Imbuia|Pinheirinho|Portão|Bairro Alto|Barreirinha|Boa Vista|Boqueirão|Cabral|Cachoeira|Caiuá|Campina do Siqueira|Campo Comprido|Capão Raso|Carmo|Centenário|CIC|Fazendinha|Hauer|Maracanã|Oficinas|Pinhais|Santa Cândida|Santa Felicidade|Sítio Cercado|Tatuquara|Guadalupe)')) then _utf8mb4'Bus terminal' when (`type` = _utf8mb3'Estação tubo') then _utf8mb4'Tube station' when (`type` = _utf8mb3'Linha Turismo') then _utf8mb4'Tourism line' when (`type` = _utf8mb3'Especial Madrugueiro') then _utf8mb4'Dawn bus' when (`type` in (_utf8mb3'Chapéu chinês',_utf8mb3'Domus',_utf8mb3'Novo mobiliário',_utf8mb3'Placa em cano',_utf8mb3'Placa em poste',_utf8mb3'Sem demarcação')) then _utf8mb4'Street bus stop' else _utf8mb4'Others' end)) VIRTUAL,
  `last_update` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `legacy_id_UNIQUE` (`legacy_id`)
) ENGINE=InnoDB AUTO_INCREMENT=22454 DEFAULT CHARSET=utf8mb3;


CREATE TABLE `dim_line` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `line_code` varchar(255) NOT NULL,
  `line_name` text,
  `service_category` text,
  `color` text,
  `last_update` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `line_code` (`line_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `fat_event` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `dim_line_id` int DEFAULT NULL,
  `vehicle` text,
  `itinerary_id` int DEFAULT NULL,
  `event_timestamp` datetime DEFAULT NULL,
  `seq` int DEFAULT NULL,
  `dim_bus_stop_id` int DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  `last_update` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `fk_dim_bus_stop_idx` (`dim_bus_stop_id`),
  KEY `fk_dim_line_idx` (`dim_line_id`),
  KEY `fat_event_base_date_idx` (`base_date`) USING BTREE,
  KEY `fat_event_itinerary_date_idx` (`itinerary_id`,`base_date`,`dim_line_id`),
  CONSTRAINT `fk_dim_bus_stop` FOREIGN KEY (`dim_bus_stop_id`) REFERENCES `dim_bus_stop` (`id`),
  CONSTRAINT `fk_dim_line` FOREIGN KEY (`dim_line_id`) REFERENCES `dim_line` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=77889160 DEFAULT CHARSET=utf8mb3;

CREATE TABLE `fat_itinerary` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `dim_line_id` int(11) DEFAULT NULL,
  `bus_stop_id` int(11) NOT NULL DEFAULT '0',
  `next_bus_stop_id` int(11) DEFAULT '0',
  `next_bus_stop_delta_s` double DEFAULT NULL,
  `itinerary_id` int(11) DEFAULT NULL,
  `seq` int(11) DEFAULT NULL,
  `line_way` varchar(255) DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  `last_update` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_dim_next_bus_stop_idx` (`next_bus_stop_id`),
  KEY `fk_dim_bus_stop_idx` (`bus_stop_id`),
  KEY `fk_dim_line_idx` (`dim_line_id`),
  KEY `fat_itinerary_base_date_idx` (`base_date`) USING BTREE,
  CONSTRAINT `fk_dim_bus_stop_1` FOREIGN KEY (`bus_stop_id`) REFERENCES `dim_bus_stop` (`id`),
  CONSTRAINT `fk_dim_line_itinerary` FOREIGN KEY (`dim_line_id`) REFERENCES `dim_line` (`id`),
  CONSTRAINT `fk_dim_next_bus_stop` FOREIGN KEY (`next_bus_stop_id`) REFERENCES `dim_bus_stop` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `fat_most_relevant_itinerary` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `line_code` varchar(255) NOT NULL DEFAULT '',
  `itinerary_id` int DEFAULT NULL,
  `base_date` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fat_mri_line_itinerary_date_idx` (`line_code`,`itinerary_id`,`base_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb3;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `fn_haversine`(lat1 double, lng1 double, lat2 double, lng2 double) RETURNS double
READS SQL DATA
BEGIN
    DECLARE R INT;
    DECLARE dLat DECIMAL(30,15);
    DECLARE dLng DECIMAL(30,15);
    DECLARE a1 DECIMAL(30,15);
    DECLARE a2 DECIMAL(30,15);
    DECLARE a DECIMAL(30,15);
    DECLARE c DECIMAL(30,15);
    DECLARE d DECIMAL(30,15);

    SET R = 6371000;
    SET dLat = RADIANS( lat2 ) - RADIANS( lat1 );
    SET dLng = RADIANS( lng2 ) - RADIANS( lng1 );
    SET a1 = SIN( dLat / 2 ) * SIN( dLat / 2 );
    SET a2 = SIN( dLng / 2 ) * SIN( dLng / 2 ) * COS( RADIANS( lng1 )) * COS( RADIANS( lat2 ) );
    SET a = a1 + a2;
    SET c = 2 * ATAN2( SQRT( a ), SQRT( 1 - a ) );
    SET d = R * c;
    RETURN d;
END$$
DELIMITER ;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_cluster` AS select `d1`.`legacy_id` AS `bus_stop_centre`,`d2`.`legacy_id` AS `bus_stop_clustered`,`busanalysis_dw`.`fn_haversine`(`d1`.`latitude`,`d1`.`longitude`,`d2`.`latitude`,`d2`.`longitude`) AS `d` from (`busanalysis_dw`.`dim_bus_stop` `d1` join `busanalysis_dw`.`dim_bus_stop` `d2`) where (`busanalysis_dw`.`fn_haversine`(`d1`.`latitude`,`d1`.`longitude`,`d2`.`latitude`,`d2`.`longitude`) <= 600);
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_event` AS select `busanalysis_dw`.`dim_line`.`line_code` AS `line_code`,`busanalysis_dw`.`dim_line`.`line_name` AS `line_name`,`fat`.`vehicle` AS `vehicle`,`dim`.`latitude` AS `latitude`,`dim`.`longitude` AS `longitude`,`dim`.`name` AS `name`,`dim`.`legacy_id` AS `legacy_id`,`dim`.`type` AS `type`,`fat`.`seq` AS `seq`,`fat`.`itinerary_id` AS `itinerary_id`,`fat`.`event_timestamp` AS `event_timestamp`,`fat`.`base_date` AS `base_date` from ((`busanalysis_dw`.`fat_event` `fat` join `busanalysis_dw`.`dim_bus_stop` `dim` on((`fat`.`dim_bus_stop_id` = `dim`.`id`))) join `busanalysis_dw`.`dim_line` on((`fat`.`dim_line_id` = `busanalysis_dw`.`dim_line`.`id`)));
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_itinerary` AS select `busanalysis_dw`.`dim_line`.`line_code` AS `line_code`,`busanalysis_dw`.`dim_line`.`line_name` AS `line_name`,`fat`.`itinerary_id` AS `itinerary_id`,`fat`.`line_way` AS `line_way`,`busanalysis_dw`.`dim_bus_stop`.`legacy_id` AS `legacy_id`,`busanalysis_dw`.`dim_bus_stop`.`name` AS `name`,`busanalysis_dw`.`dim_bus_stop`.`type` AS `type`,`busanalysis_dw`.`dim_bus_stop`.`latitude` AS `latitude`,`busanalysis_dw`.`dim_bus_stop`.`longitude` AS `longitude`,`dim_next_bus_stop`.`name` AS `next_bus_stop_name`,`dim_next_bus_stop`.`legacy_id` AS `next_bus_stop_legacy_id`,`fat`.`next_bus_stop_delta_s` AS `next_bus_stop_delta_s`,`fat`.`seq` AS `seq`,`fat`.`base_date` AS `base_date` from (((`busanalysis_dw`.`fat_itinerary` `fat` join `busanalysis_dw`.`dim_bus_stop` on((`fat`.`bus_stop_id` = `busanalysis_dw`.`dim_bus_stop`.`id`))) left join `busanalysis_dw`.`dim_bus_stop` `dim_next_bus_stop` on((`fat`.`next_bus_stop_id` = `dim_next_bus_stop`.`id`))) join `busanalysis_dw`.`dim_line` on((`fat`.`dim_line_id` = `busanalysis_dw`.`dim_line`.`id`)));
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_event_mri` AS select `busanalysis_dw`.`vw_event`.`line_code` AS `line_code`,`busanalysis_dw`.`vw_event`.`line_name` AS `line_name`,`busanalysis_dw`.`vw_event`.`vehicle` AS `vehicle`,`busanalysis_dw`.`vw_event`.`latitude` AS `latitude`,`busanalysis_dw`.`vw_event`.`longitude` AS `longitude`,`busanalysis_dw`.`vw_event`.`name` AS `name`,`busanalysis_dw`.`vw_event`.`legacy_id` AS `legacy_id`,`busanalysis_dw`.`vw_event`.`type` AS `type`,`busanalysis_dw`.`vw_event`.`seq` AS `seq`,`busanalysis_dw`.`vw_event`.`itinerary_id` AS `itinerary_id`,`busanalysis_dw`.`vw_event`.`event_timestamp` AS `event_timestamp`,`busanalysis_dw`.`vw_event`.`base_date` AS `base_date` from (`busanalysis_dw`.`vw_event` join `busanalysis_dw`.`fat_most_relevant_itinerary` `fat` on(((`busanalysis_dw`.`vw_event`.`line_code` = `fat`.`line_code`) and (`busanalysis_dw`.`vw_event`.`itinerary_id` = `fat`.`itinerary_id`) and (`busanalysis_dw`.`vw_event`.`base_date` = `fat`.`base_date`))));
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `busanalysis_dw`.`vw_itinerary_mri` AS select `busanalysis_dw`.`vw_itinerary`.`line_code` AS `line_code`,`busanalysis_dw`.`vw_itinerary`.`line_name` AS `line_name`,`busanalysis_dw`.`vw_itinerary`.`itinerary_id` AS `itinerary_id`,`busanalysis_dw`.`vw_itinerary`.`legacy_id` AS `legacy_id`,`busanalysis_dw`.`vw_itinerary`.`name` AS `name`,`busanalysis_dw`.`vw_itinerary`.`type` AS `type`,`busanalysis_dw`.`vw_itinerary`.`latitude` AS `latitude`,`busanalysis_dw`.`vw_itinerary`.`longitude` AS `longitude`,`busanalysis_dw`.`vw_itinerary`.`next_bus_stop_name` AS `next_bus_stop_name`,`busanalysis_dw`.`vw_itinerary`.`next_bus_stop_legacy_id` AS `next_bus_stop_legacy_id`,`busanalysis_dw`.`vw_itinerary`.`next_bus_stop_delta_s` AS `next_bus_stop_delta_s`,`busanalysis_dw`.`vw_itinerary`.`seq` AS `seq`,`busanalysis_dw`.`vw_itinerary`.`base_date` AS `base_date` from (`busanalysis_dw`.`vw_itinerary` join `busanalysis_dw`.`fat_most_relevant_itinerary` `fat` on(((`busanalysis_dw`.`vw_itinerary`.`line_code` = `fat`.`line_code`) and (`busanalysis_dw`.`vw_itinerary`.`itinerary_id` = `fat`.`itinerary_id`) and (`busanalysis_dw`.`vw_itinerary`.`base_date` = `fat`.`base_date`))));

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_dim_bus_stop`()
BEGIN
	TRUNCATE busanalysis_etl.etl_dim_bus_stop;
	INSERT INTO busanalysis_etl.etl_dim_bus_stop
	SELECT
		id
        ,MAX(name)
		,AVG(latitude)
		,AVG(longitude)
		,MAX(type)
	FROM busanalysis_etl.etl_itinerary
    GROUP BY id;

	INSERT INTO busanalysis_dw.dim_bus_stop(legacy_id, name, latitude, longitude, type)
	SELECT
		etl_dim.id
		,etl_dim.name
		,etl_dim.latitude
		,etl_dim.longitude
		,etl_dim.type
	FROM busanalysis_etl.etl_dim_bus_stop AS etl_dim
	LEFT JOIN busanalysis_dw.dim_bus_stop AS dim ON etl_dim.id = dim.legacy_id
	WHERE dim.legacy_id IS NULL;

	UPDATE busanalysis_dw.dim_bus_stop AS dim
	INNER JOIN busanalysis_etl.etl_dim_bus_stop AS etl_dim ON dim.legacy_id = etl_dim.id
	SET
		dim.name = etl_dim.name
		,dim.latitude = etl_dim.latitude
		,dim.longitude = etl_dim.longitude
		,dim.type = etl_dim.type
		,dim.last_update = current_timestamp()
	WHERE
		dim.name <> etl_dim.name
		OR dim.latitude <> etl_dim.latitude
		OR dim.longitude <> etl_dim.longitude
		OR dim.type <> etl_dim.type;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_dim_line`()
BEGIN
	INSERT INTO busanalysis_dw.dim_line(line_code, line_name, service_category, color)
	SELECT
		etl_dim.line_code
		,etl_dim.line_name
		,etl_dim.service_category
		,etl_dim.color
	FROM busanalysis_etl.etl_line AS etl_dim
	LEFT JOIN busanalysis_dw.dim_line AS dim ON etl_dim.line_code = dim.line_code
	WHERE dim.line_code IS NULL;

	UPDATE busanalysis_dw.dim_line AS dim
	INNER JOIN busanalysis_etl.etl_line AS etl_dim ON dim.line_code = etl_dim.line_code
	SET
		dim.line_name = etl_dim.line_name
		,dim.service_category = etl_dim.service_category
		,dim.color = etl_dim.color
		,dim.last_update = current_timestamp()
	WHERE
		dim.line_name <> etl_dim.line_name
		OR dim.service_category <> etl_dim.service_category
		OR dim.color <> etl_dim.color;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_fat_itinerary`(IN base_date_in DATE)
BEGIN
	TRUNCATE busanalysis_etl.etl_fat_itinerary;
	INSERT INTO busanalysis_etl.etl_fat_itinerary
	SELECT
		line_code
		,id
		,next_stop_id
		,next_stop_delta_s
		,itinerary_id
		,seq
		,line_way
	FROM busanalysis_etl.etl_itinerary;

	REPEAT
		DELETE FROM busanalysis_dw.fat_itinerary
		WHERE
			base_date = base_date_in
		LIMIT 10000;
	UNTIL ROW_COUNT() = 0 END REPEAT;

	INSERT INTO busanalysis_dw.fat_itinerary(dim_line_id, bus_stop_id, next_bus_stop_id, next_bus_stop_delta_s, itinerary_id, seq, line_way, base_date)
	SELECT
		dim_line.id
		,dim.id AS bus_stop_id
		,dim_next.id AS next_bus_stop_id
		,next_stop_delta_s
		,itinerary_id
		,seq
		,line_way
        ,base_date_in
	FROM busanalysis_etl.etl_fat_itinerary AS etl_fat
	INNER JOIN busanalysis_dw.dim_bus_stop AS dim ON dim.legacy_id = etl_fat.id
	LEFT JOIN busanalysis_dw.dim_bus_stop AS dim_next ON dim_next.legacy_id = etl_fat.next_stop_id
    INNER JOIN busanalysis_dw.dim_line AS dim_line ON dim_line.line_code = etl_fat.line_code;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_fat_event`(IN base_date_in DATE)
BEGIN
	REPEAT
		DELETE FROM busanalysis_dw.fat_event
		WHERE
			base_date = base_date_in
		LIMIT 10000;
	UNTIL ROW_COUNT() = 0 END REPEAT;
    
	INSERT INTO busanalysis_dw.fat_event(dim_line_id, vehicle, itinerary_id, event_timestamp, seq, dim_bus_stop_id, base_date)
	SELECT
		dim_line.id
		,evt.vehicle
		,evt.itinerary_id
		,CAST(evt.event_timestamp AS DATETIME) AS event_timestamp
        ,evt.seq
		,dim.id AS dim_bus_stop_id
		,base_date_in
	FROM busanalysis_etl.etl_event AS evt
	INNER JOIN busanalysis_dw.dim_bus_stop AS dim ON dim.legacy_id = evt.id
    INNER JOIN busanalysis_dw.dim_line AS dim_line ON dim_line.line_code = evt.line_code;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_load_fat_most_relevant_itinerary`(IN base_date_in DATE)
BEGIN
	DELETE FROM busanalysis_dw.fat_most_relevant_itinerary
	WHERE
		base_date = base_date_in;
        
	INSERT INTO busanalysis_dw.fat_most_relevant_itinerary(line_code, itinerary_id, base_date)
	WITH ItineraryCounts AS (
		SELECT DISTINCT dim_line_id, base_date, COUNT(DISTINCT itinerary_id) AS itinerary_counts
		FROM fat_itinerary
		WHERE
			base_date = base_date_in
		GROUP BY dim_line_id, base_date
	),
	MostRelevantItineraries AS (
		SELECT fat_event.dim_line_id, itinerary_id, COUNT(*) AS count, fat_event.base_date
		FROM fat_event
			INNER JOIN ItineraryCounts tbl ON tbl.dim_line_id = fat_event.dim_line_id AND tbl.base_date = fat_event.base_date
		WHERE itinerary_counts > 2
		GROUP BY fat_event.base_date, fat_event.dim_line_id, itinerary_id
	),
	ItineraryPairs AS (
	SELECT 
		A.dim_line_id,    
		A.itinerary_id itinerary_id_A,
		A.count count_A, 
		B.itinerary_id itinerary_id_B,
		B.count count_B, 
		A.base_date
	FROM MostRelevantItineraries A, MostRelevantItineraries B
	WHERE
		A.dim_line_id = B.dim_line_id
		AND A.itinerary_id != B.itinerary_id
	), 
	EventsQuantity AS (
	SELECT 
		ItineraryPairs.*,
		ROUND(
			COUNT(DISTINCT fat_event.dim_line_id, vehicle, dim_bus_stop_id, event_timestamp) /
			COUNT(DISTINCT fat_event.dim_line_id, vehicle, fat_event.itinerary_id, dim_bus_stop_id, event_timestamp),
			2
		) AS rate
	FROM fat_event, ItineraryPairs
	WHERE
		fat_event.dim_line_id = ItineraryPairs.dim_line_id
        AND fat_event.base_date = ItineraryPairs.base_date
		AND fat_event.itinerary_id IN (ItineraryPairs.itinerary_id_A, ItineraryPairs.itinerary_id_B)
	GROUP BY 
		fat_event.dim_line_id, 
        ItineraryPairs.itinerary_id_A, 
        ItineraryPairs.count_A,
        ItineraryPairs.itinerary_id_B,
        ItineraryPairs.count_B,
        ItineraryPairs.base_date
	),
	BestCandidates AS
	(
		SELECT 
			*, 
			ROW_NUMBER() OVER (PARTITION BY EventsQuantity.dim_line_id ORDER BY rate DESC, count_a DESC, count_b DESC) AS row_num
		FROM EventsQuantity
	)
	SELECT 
		line_code,
		itinerary_id, 
		BestCandidates.base_date
	FROM BestCandidates
	INNER JOIN MostRelevantItineraries ON 
		MostRelevantItineraries.dim_line_id = BestCandidates.dim_line_id
	INNER JOIN dim_line ON dim_line.id = MostRelevantItineraries.dim_line_id    
	WHERE
		BestCandidates.row_num = 1
		AND (
				(BestCandidates.rate >= 0.8 AND MostRelevantItineraries.itinerary_id IN (itinerary_id_A, itinerary_id_B))
				OR
				(BestCandidates.rate < 0.8 AND MostRelevantItineraries.itinerary_id = itinerary_id_A)
			)

	UNION ALL

	SELECT DISTINCT dim_line.line_code, fat_itinerary.itinerary_id, tbl.base_date
	FROM fat_itinerary
	INNER JOIN dim_line ON dim_line.id = fat_itinerary.dim_line_id
	INNER JOIN ItineraryCounts tbl ON tbl.dim_line_id = dim_line.id AND tbl.base_date = fat_itinerary.base_date
	WHERE
		itinerary_counts <= 2;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_load_all`(IN base_date DATE)
BEGIN
	CALL busanalysis_dw.sp_load_dim_bus_stop();
    CALL busanalysis_dw.sp_load_dim_line();
	CALL busanalysis_dw.sp_load_fat_event(base_date);
	CALL busanalysis_dw.sp_load_fat_itinerary(base_date);
END$$
DELIMITER ;