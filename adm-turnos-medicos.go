package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"time"
)

func createDataBase() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`drop database if exists adm_turnos_medicos`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create database adm_turnos_medicos`)
	if err != nil {
        log.Fatal(err)
    }
}

func crearTablas(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
    }
    defer db.Close()
    
    _,err = db.Exec(`
    create table paciente(
		nro_paciente	int,
		nombre		text,
		apellido	text,
		dni_paciente	int,
		f_nac		date,
		nro_obra_social	int,
		nro_afiliade	int,
		domicilio	text,
		telefono	char(12),
		email		text
	);
	create table medique(
		dni_medique		int,
		nombre			text,
		apellido		text,
		especialidad		varchar(64),
		monto_consulta_privada	decimal(12,2),
		telefono		char(12)
	);

	create table consultorio(
		nro_consultorio	int,
		nombre		text,
		domicilio	text,
		codigo_postal	char(8),
		telefono	char(12)
	);

	create table agenda(
		dni_medique	int,
		dia		int,
		nro_consultorio	int,
		hora_desde	time,
		hora_hasta	time,
		duracion_turno	interval
	);

	create table turno(
		nro_turno			serial, 
		fecha				timestamp, 
		nro_consultorio			int, 
		dni_medique			int, 
		nro_paciente			int, 
		nro_obra_social_consulta	int, 
		nro_afiliade_consulta		int, 
		monto_paciente			decimal(12, 2), 
		monto_obra_social		decimal(12, 2), 
		f_reserva			timestamp, 
		estado				char(10)
	);

	create table reprogramacion(
		nro_turno		int, 
		nombre_paciente		text, 
		apellido_paciente	text, 
		telefono_paciente	char(12), 
		email_paciente		text, 
		nombre_medique		text, 
		apellido_medique	text, 
		estado			char(12)
	);

	create table error(
		nro_error	serial, 
		f_turno		timestamp, 
		nro_consultorio	int, 
		dni_medique	int, 
		nro_paciente	int, 
		operacion	char(12), 
		f_error		timestamp, 
		motivo		varchar(64)
	);

	create table cobertura(
		dni_medique		int, 
		nro_obra_social		int, 
		monto_paciente		decimal(12, 2), 
		monto_obra_social	decimal(12, 2)
	);

	create table obra_social(
		nro_obra_social		int,
		nombre			varchar,
		contacto_nombre		varchar,
		contacto_apellido	varchar,
		contacto_telefono  char(12),
		contacto_email	 	varchar
	);

	create table liquidacion_cabecera(
		nro_liquidacion		serial,
		nro_obra_social		int,
		desde			date ,
		hasta	 		date,
		total			decimal(15,2)
	);

	create table liquidacion_detalle(
		nro_liquidacion	int,
		nro_linea 		int,
		f_atencion 		date,
		nro_afiliade 		int,
		dni_paciente 		int,
		nombre_paciente 	varchar,
		apellido_paciente 	varchar,
		dni_medique 		int,
		nombre_medique 		varchar,
		apellido_medique	varchar,
		especialidad	 	varchar(64),
		monto	 		decimal(12,2)
	);

	create table envio_email(
		nro_email	 serial,
		f_generacion	 timestamp,
		email_paciente	 text,
		asunto 	   	 text,
		cuerpo	 	 text,
		f_envio	 	 timestamp,
		estado	 	 char(10)
	);

	create table solicitud_reservas(
		nro_orden 	 int, 
		nro_paciente int, 
		dni_medique  int, 
		fecha 		 date, 
		hora 		 time
	);
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func crearPkFk(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_,err = db.Exec(
	`
	alter table paciente add constraint paciente_pk primary key(nro_paciente);
	alter table medique add constraint medique_pk primary key(dni_medique);
	alter table consultorio add constraint consultorio_pk primary key(nro_consultorio);
	alter table agenda add constraint agenda_pk primary key(dni_medique, dia);
	alter table turno add constraint turno_pk primary key(nro_turno);
	alter table reprogramacion add constraint reprogramacion_pk primary key(nro_turno);
	alter table error add constraint error_pk primary key(nro_error);
	alter table cobertura add constraint cobertura_pk primary key(dni_medique, nro_obra_social);
	alter table obra_social add constraint obra_social_pk primary key(nro_obra_social);
	alter table liquidacion_cabecera add constraint liquidacion_cabecera_pk primary key(nro_liquidacion);
	alter table liquidacion_detalle add constraint liquidacion_detalle_pk primary key(nro_liquidacion, nro_linea);
	alter table envio_email add constraint envio_email_pk primary key(nro_email);
	
	alter table paciente add constraint paciente_nro_obra_social_fk foreign key(nro_obra_social) references obra_social(nro_obra_social);
	alter table agenda add constraint agenda_dni_medique_fk foreign key(dni_medique) references medique(dni_medique);
	alter table agenda add constraint agenda_nro_consultorio_fk foreign key(nro_consultorio) references consultorio(nro_consultorio);
	alter table turno add constraint turno_nro_consultorio_fk foreign key(nro_consultorio) references consultorio(nro_consultorio);
	alter table turno add constraint turno_dni_medique_fk foreign key(dni_medique) references medique(dni_medique);
	alter table turno add constraint turno_nro_paciente_fk foreign key(nro_paciente) references paciente(nro_paciente);
	alter table turno add constraint turno_nro_obra_social_consulta_fk foreign key(nro_obra_social_consulta) references obra_social(nro_obra_social);
	alter table error add constraint error_nro_consultorio_fk foreign key(nro_consultorio) references consultorio(nro_consultorio);
	alter table error add constraint error_dni_medique_fk foreign key(dni_medique) references medique(dni_medique);
	alter table error add constraint error_nro_paciente_fk foreign key(nro_paciente) references paciente(nro_paciente);
	alter table cobertura add constraint cobertura_dni_medique_fk foreign key(dni_medique) references medique(dni_medique);
	alter table cobertura add constraint cobertura_nro_obra_social_fk foreign key(nro_obra_social) references obra_social(nro_obra_social);
	alter table liquidacion_cabecera add constraint liquidacion_cabecera_nro_obra_social_fk foreign key(nro_obra_social) references obra_social(nro_obra_social);
	alter table liquidacion_detalle add constraint liquidacion_detalle_nro_liquidacion_fk foreign key(nro_liquidacion) references liquidacion_cabecera(nro_liquidacion);
	alter table liquidacion_detalle add constraint liquidacion_detalle_dni_medique_fk foreign key(dni_medique) references medique(dni_medique);
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func borrarPkFk(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`
	alter table liquidacion_detalle drop constraint liquidacion_detalle_dni_medique_fk;
	alter table liquidacion_detalle drop constraint liquidacion_detalle_nro_liquidacion_fk;
	alter table liquidacion_cabecera drop constraint liquidacion_cabecera_nro_obra_social_fk;
	alter table cobertura drop constraint cobertura_nro_obra_social_fk;
	alter table cobertura drop constraint cobertura_dni_medique_fk;
	alter table error drop constraint error_nro_paciente_fk;
	alter table error drop constraint error_dni_medique_fk;
	alter table error drop constraint error_nro_consultorio_fk;
	alter table turno drop constraint turno_nro_obra_social_consulta_fk;
	alter table turno drop constraint turno_nro_paciente_fk;
	alter table turno drop constraint turno_dni_medique_fk;
	alter table turno drop constraint turno_nro_consultorio_fk;
	alter table agenda drop constraint agenda_nro_consultorio_fk;
	alter table agenda drop constraint agenda_dni_medique_fk;
	alter table paciente drop constraint paciente_nro_obra_social_fk;
	
	alter table envio_email drop constraint envio_email_pk;
	alter table liquidacion_detalle drop constraint liquidacion_detalle_pk;
	alter table liquidacion_cabecera drop constraint liquidacion_cabecera_pk;
	alter table obra_social drop constraint obra_social_pk;
	alter table cobertura drop constraint cobertura_pk;
	alter table error drop constraint error_pk;
	alter table reprogramacion drop constraint reprogramacion_pk;
	alter table turno drop constraint turno_pk;
	alter table agenda drop constraint agenda_pk;
	alter table consultorio drop constraint consultorio_pk;
	alter table medique drop constraint medique_pk;
	alter table paciente drop constraint paciente_pk;	
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func completarTablas(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_,err = db.Exec(
	`
	insert into obra_social values(701125, 'OSECAC', 'Juliana', 'Rodriguez', '11-0022-4789', 'info@osecac.org.ar');
	insert into obra_social values(003153, 'OSDOP', 'Gustavo', 'Gian', '11-0033-2521','info@osdop.org.ar');
	insert into obra_social values(301070, 'IOMA', 'Mariana', 'Cabezas', '11-0044-5824','info@ioma.org.ar');
	insert into obra_social values(010235, 'OSPACA', 'Diego', 'Altamirano', '11-0055-8630','info@ospaca.org.ar');
	insert into obra_social values(162504, 'OSCOEMA', 'Francisco', 'Fernandez', '11-0066-0889','info@oscoema.org.ar');

	insert into paciente values(1, 'Amparo', 'Carranza', 35297524, '1992-05-07', 701125 , 7524, 'Carlos Pellegrini 451', 11-6409-5604, 'acarranza92@gmail.com');
	insert into paciente values(2, 'Zahra', 'Bernabeu', 43620390, '2001-08-26', 003153 , 0390, 'Cuenca 3384', 11-2462-2971, 'zbernadeu01@gmail.com');
	insert into paciente values(3, 'Elizabeth', 'Paredes', 19830720, '1973-10-25', 301070 , 0720, 'Córdoba 1765', 11-431-5272, 'eparedes73@gmail.com');
	insert into paciente values(4, 'Samuel', 'Blanco', 39704633, '1996-09-24', 010235 , 4633, 'Florida 570', 11-0687-3103, 'sblanco96@gmail.com');
	insert into paciente values(5, 'Gerardo', 'Heras', 15607564, '1962-01-23', 162504 , 7564, 'San Juan 2826', 11-6471-6439, 'gheras62@gmail.com');
	insert into paciente values(6, 'Maria', 'Gomez', 31536662, '1984-12-08', 701125 , 6662, 'Arenales 3360', 11-7155-2681, 'mgomez84@gmail.com');
	insert into paciente values(7, 'Roman', 'Ferrer', 42833745, '2002-05-05', 003153 , 3745, 'Rivadavia 7302', 11-1193-8468, 'rferrer02@gmail.com');
	insert into paciente values(8, 'Bruno', 'Mestre', 37844136, '1994-06-27', 301070 ,4136, 'Lavalle 964', 11-3356-9195, 'bmestre94@gmail.com');
	insert into paciente values(9, 'Jennifer', 'Aranda', 32968962, '1985-10-25', 010235, 8962, 'Las Heras 2601', 11-272-4927, 'jaranda85@gmail.com'); 
	insert into paciente values(10, 'Eric', 'Gilabert', 13615890, '1960-05-14', 162504, 5890, 'Moreno 877', 11-5617-5261, 'egilabert60@gmail.com');
	insert into paciente values(11, 'Micaela', 'Romero', 26718210, '1978-09-06', 701125, 8210, 'Maipu 1270', 11-4512-9047, 'mromero78@gmail.com');
	insert into paciente values(12, 'Roberto', 'Mejias', 35953162, '1992-08-29', 003153, 3162, 'Federico Lacroze 2301', 11-9619-1362, 'rmejias92@gmail.com');
	insert into paciente values(13, 'Jose', 'Pico', 25586321, '1979-06-05', 301070, 6321, 'Bouchard 685', 11-9948-2446, 'jpico79@gmail.com');
	insert into paciente values(14, 'Judit', 'Silvestre', 11994960, '1958-03-04', 010235, 4960, 'Godoy Cruz 2560', 11-5235-4229, 'jsilvestre58@gmail.com');
	insert into paciente values(15, 'Vanesa', 'Silva', 31812130, '1984-01-21', 162504, 2130, 'Cabildo 702', 11-5997-9352, 'vsilva84@gmail.com');
	insert into paciente values(16, 'Gaspar', 'Orozco', 15611929, '1962-12-01', 701125, 1929, 'Callao 1501', 11-6323-6581, 'gorozco62@gmail.com');
	insert into paciente values(17, 'Ramiro', 'Alfonso', 38129076, '1995-05-17', 003153, 9076, 'Triunvirato 4402', 11-1715-5090, 'ralfonso95@gmail.com');
	insert into paciente values(18, 'Sonia', 'Bartolome', 19268700, '1966-11-02', 301070, 8700, 'Carlos Pellegrini 435', 11-2543-8703, 'sbartolome66@gmail.com');
	insert into paciente values(19, 'Caridad', 'Borras', 24106382, '1980-07-07', 010235, 6382, 'Viamonte 545', 11-2543-8703, 'cborras80@gmail.com');
	insert into paciente values(20, 'Adrian', 'Vasquez', 34727887, '1987-09-28', 162504, 7887, 'Montevideo 400', 11-6122-2873, 'avasquez87@gmail.com');

	insert into medique values(35428517, 'Julian', 'Gonzalez', 'cardiologia', 5000.00, '11-3257-4789');
	insert into medique values(20250305, 'Gabriela', 'Paez', '', 2500.00, '11-7766-2521');
	insert into medique values(27879111, 'Lautaro', 'Sabatte', '', 2500.00, '11-5596-5824');
	insert into medique values(39331248, 'Soledad', 'Mesa', '', 2000.00, '11-6970-8630');
	insert into medique values(33002441, 'Delfina', 'Maidana', 'ginecologia', 4000.00, '11-2274-0889');
	insert into medique values(20325879, 'Silvia', 'Castro', 'osteopatia', 7000.00, '11-8837-4449');
	insert into medique values(27442789, 'Analia', 'Saitta', '', 2700.00, '11-4578-1112');
	insert into medique values(22789111, 'Mariela', 'Hernandez', '', 2500.00, '11-2270-3211');
	insert into medique values(19312775, 'Carlos', 'Bravo', '', 2300.00, '11-0051-4039');
	insert into medique values(30504102, 'Esteban', 'Cardozo', '', 2400.00, '11-8880-4752');
	insert into medique values(35437112, 'Leandro', 'Nuñez', '', 2100.00, '11-0598-1452');
	insert into medique values(26779151, 'Juana', 'Gimenez', '', 2500.00, '11-1122-3344');
	insert into medique values(21248744, 'Maria', 'Castillo', '', 2550.00, '11-0051-4039');
	insert into medique values(36777999, 'Natalia', 'Dubreucq', '', 2800.00, '11-7963-5541');
	insert into medique values(40205348, 'Benjamin', 'Pratto', '', 2500.00, '11-4545-7739');
	insert into medique values(23487555, 'Nicolas', 'Batter', '', 2450.00, '11-9632-7412');
	insert into medique values(38974125, 'Eliana', 'Farias', '', 1900.00, '11-0558-1453');
	insert into medique values(16789111, 'Alfredo', 'Rios', '', 2000.00, '11-7412-7771');
	insert into medique values(40787211, 'Sofia', 'Pereira', '', 2500.00, '11-1851-2231');
	insert into medique values(34789362, 'Ana', 'Garcia', '', 2500.00, '11-4762-1541');

	insert into consultorio values(1, 'Sarmiento', 'Peron 1207', 'B1663SAN', '11-4455-7900');
	insert into consultorio values(2, 'Roca', 'Arias 2369', 'B1712MOR', '11-4455-8000');
	insert into consultorio values(3, 'Bessone', 'Libertador 487', 'B1722MER', '11-4455-8100');
	insert into consultorio values(4, 'Austral', 'Zufrietegui 965', 'B1714GDM', '11-4455-8200');
	insert into consultorio values(5, 'Merced', 'Yrigoyen 1740', 'B1665JOS', '11-4455-8300');

	insert into agenda values(35428517, 1, 1, '09:00:00','15:00:00', '45 minutes');
	insert into agenda values(20250305, 2, 1, '07:45:00','12:00:00', '15 minutes');
	insert into agenda values(20250305, 5, 1, '09:00:00','14:00:00', '30 minutes');
	insert into agenda values(27879111, 3, 1, '08:00:00','12:00:30', '20 minutes');
	insert into agenda values(39331248, 4, 1, '13:00:00','17:00:30', '15 minutes');
	insert into agenda values(33002441, 1, 2, '12:00:00','16:00:00', '20 minutes');
	insert into agenda values(33002441, 3, 2, '10:00:00','14:00:00', '20 minutes');
	insert into agenda values(20325879, 2, 2, '13:00:00','19:00:00', '45 minutes');
	insert into agenda values(27442789, 4, 2, '08:00:00','12:00:00', '30 minutes');
	insert into agenda values(22789111, 5, 2, '11:00:00','15:00:00', '20 minutes');
	insert into agenda values(19312775, 1, 3, '13:00:00','17:00:00', '20 minutes');
	insert into agenda values(30504102, 2, 3, '14:00:00','18:30:00', '15 minutes');
	insert into agenda values(30504102, 4, 3, '14:00:00','17:30:00', '15 minutes');
	insert into agenda values(35437112, 3, 3, '12:00:00','16:40:00', '20 minutes');
	insert into agenda values(26779151, 5, 3, '12:00:00','15:30:00', '15 minutes');
	insert into agenda values(21248744, 1, 4, '12:00:00','16:00:00', '15 minutes');
	insert into agenda values(36777999, 2, 4, '14:00:00','18:00:00', '30 minutes');
	insert into agenda values(40205348, 3, 4, '11:00:00','18:00:00', '20 minutes');
	insert into agenda values(23487555, 4, 4, '12:00:00','18:00:00', '15 minutes');
	insert into agenda values(38974125, 2, 5, '12:30:00','17:00:00', '30 minutes');
	insert into agenda values(16789111, 3, 5, '14:00:00','19:00:00', '20 minutes');
	insert into agenda values(40787211, 4, 5, '11:30:00','15:00:00', '15 minutes');
	insert into agenda values(34789362, 5, 5, '09:00:00','13:00:00', '20 minutes');

	insert into cobertura values(35428517, 701125, 1000.00, 1500.00);
	insert into cobertura values(20250305, 003153, 700.00, 1700.00);
	insert into cobertura values(27879111, 301070, 500.00, 1700.00);
	insert into cobertura values(39331248, 010235, 00.00, 1500.00);
	insert into cobertura values(33002441, 162504, 500.00, 2000.00);
	insert into cobertura values(20325879, 701125, 1000.00, 1500.00);
	insert into cobertura values(27442789, 003153, 700.00, 1700.00);
	insert into cobertura values(22789111, 301070, 500.00, 1700.00);
	insert into cobertura values(19312775, 010235, 750.00, 1500.00);
	insert into cobertura values(30504102, 162504, 500.00, 2000.00);
	insert into cobertura values(35437112, 701125, 1000.00, 1500.00);
	insert into cobertura values(26779151, 003153, 700.00, 1700.00);
	insert into cobertura values(21248744, 301070, 500.00, 1700.00);
	insert into cobertura values(36777999, 010235, 750.00, 1500.00);
	insert into cobertura values(40205348, 162504, 00.00, 2000.00);
	insert into cobertura values(23487555, 701125, 1000.00, 1500.00);
	insert into cobertura values(38974125, 003153, 700.00, 1700.00);
	insert into cobertura values(16789111, 301070, 500.00, 1700.00);
	insert into cobertura values(40787211, 010235, 750.00, 1500.00);
	insert into cobertura values(34789362, 162504, 450.00, 2000.00);
	insert into cobertura values(35428517, 010235, 750.00, 1500.00);
	insert into cobertura values(20250305, 162504, 900.00, 2000.00);

	insert into solicitud_reservas values(1, 20, 35111111, '2023-06-12', '17:00:00'); --medique inexistente 
	insert into solicitud_reservas values(2, 27, 35428517, '2023-07-12', '13:30:00'); --paciente inexistente
	insert into solicitud_reservas values(3, 9, 35428517, '2023-06-12', '17:00:00'); -- obra social no atendida por medique
	insert into solicitud_reservas values(4, 4, 40787211, '2023-06-19', '10:00:00'); -- turno inexistente
	insert into solicitud_reservas values(5, 4, 40787211, '2023-06-19', '09:30:00'); --turno inexistente
	insert into solicitud_reservas values(6, 9, 40787211, '2023-06-05', '09:00:00'); -- turno 'reservado'
	insert into solicitud_reservas values(7, 4, 40787211, '2023-06-05', '09:00:00'); --turno no disponible 
	insert into solicitud_reservas values(8, 4, 40787211, '2023-06-26', '09:45:00');
	insert into solicitud_reservas values(9, 4, 40787211, '2023-06-12', '09:45:00');
	insert into solicitud_reservas values(10, 4, 40787211, '2023-06-19', '09:45:00');
	insert into solicitud_reservas values(11, 4, 40787211, '2023-06-12', '10:30:00');
	insert into solicitud_reservas values(12, 4, 40787211, '2023-06-12', '11:15:00'); --supera el lìmite de reservas
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func cargarFuncion_generar_turnos_disponibles(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`
	create function generar_turnos_disponibles(anio int, mes int) returns boolean as $$
	declare
		t_interval interval;
		t timestamp;
		v record;
		first_day date;
		last_day date;
		actual_day date;
		day_week int;
		day_agenda int;
		turno_existente turno%rowtype;
	begin 
		select * into turno_existente from turno t where extract(month from t.fecha) = mes and extract(year from t.fecha) = anio;
		if found then 
			return false;	
		end if;	

		first_day := dar_primer_dia_mes(anio,mes);
		last_day := dar_ultimo_dia_mes(anio,mes);
		actual_day := first_day;
		day_week := 0;
		day_agenda := 0;
		t_interval:= '0 hour';
		t := '1900-01-01 00:00:00';

		while actual_day <= last_day loop
			day_week = consultar_dia_semana(actual_day);
			for v in select * from agenda loop
				day_agenda = v.dia;
				if day_week = day_agenda then 
					t_interval = v.duracion_turno;
					t = actual_day + v.hora_desde;
					while t<(actual_day + v.hora_hasta) loop
						insert into turno values(default,t,v.nro_consultorio,v.dni_medique,null,null,null,null,null,null,'disponible');
						t = t + t_interval;
					end loop;
					
				end if;
				
			end loop;
				actual_day = actual_day + interval '1 day';
		end loop;
		return true;
	end;
	$$language plpgsql;
					
	--Funciones auxiliares
	create function dar_primer_dia_mes(anio int, mes int) returns date as $$
	declare
		fecha_cargada  date;
		
	begin
		fecha_cargada := make_date(anio, mes,'1');
		return fecha_cargada;	
	end;
	$$language plpgsql;

	create function dar_ultimo_dia_mes(anio int, mes int) returns date as $$
		declare 
			primer_dia   date;
			ultimo_dia    date;
		begin
			if mes = 2 then
				primer_dia := make_date(anio,'3','1');
				ultimo_dia := make_date(primer_dia - interval '1 day');
				return ultimo_dia;
			end if;
		
			primer_dia := make_date(anio,mes, '1');
			ultimo_dia  := primer_dia + interval '1 month' - interval '1 day';
			return  ultimo_dia;	
	end;
	$$ language plpgsql;

	create function consultar_dia_semana(fecha_consulta date) returns int as $$
		declare 
			dia int;
		begin
			dia := extract (isodow from fecha_consulta);
					return dia;
		end;
	$$language plpgsql;
	`)
	if err != nil {
		log.Fatal(err)
	}
}
	
func cargarFuncion_reserva_de_turno(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`
	--funcion reserva de turno
	create function reserva_de_turno(nro_his_clinica int, dni_med int, fecha_ingresada date, hora_ingresada time) returns boolean as $$
		declare
			fecha_hora timestamp;
			medique_select medique%rowtype;
			paciente_select paciente%rowtype;
			cobertura_select cobertura%rowtype;
			turno_select turno%rowtype;
			turnos_reservados int;
		begin
			fecha_hora := fecha_ingresada + hora_ingresada;
			
			select * into medique_select from medique m where m.dni_medique = dni_med;
			
			if not found then 
				insert into error values(default, fecha_hora, null , null, null, 'reserva', current_timestamp, '?dni de medique no valida');
				return false;
			end if;
			
			select * into paciente_select from paciente p where p.nro_paciente = nro_his_clinica;
			
			if not found then 
				insert into error values(default, fecha_hora, null , dni_med, null, 'reserva', current_timestamp, '?nro de historia historia clinica no valido');
				return false;
			end if; 
			
			select * into cobertura_select from cobertura c where c.dni_medique = dni_med and c.nro_obra_social = paciente_select.nro_obra_social;
			
			if not found then
				insert into error values(default, fecha_hora, null , dni_med, nro_his_clinica, 'reserva', current_timestamp, '?obra social de paciente no atendida por le medique');
				return false;
			end if; 

			select * into turno_select from turno t where t.fecha = fecha_hora and t.estado = 'disponible';
			
			if not found then
				insert into error values(default, fecha_hora, null , dni_med, nro_his_clinica, 'reserva', current_timestamp, '?turno inexistente o no disponible');
				return false;
			end if;
			
			turnos_reservados := count(*) from turno t where t.estado = 'reservado' and t.nro_paciente = nro_his_clinica;
			
			if turnos_reservados >= 5 then
				insert into error values(default, fecha_hora, null , dni_med, nro_his_clinica, 'reserva', current_timestamp, '?supera el limite de reserva de turnos');
				return false;
			end if;
			update turno t set nro_paciente = nro_his_clinica, nro_obra_social_consulta = paciente_select.nro_obra_social, nro_afiliade_consulta = paciente_select.nro_afiliade,
			monto_paciente = cobertura_select.monto_paciente, monto_obra_social = cobertura_select.monto_obra_social, f_reserva = current_timestamp, estado = 'reservado'
			where t.nro_turno = turno_select.nro_turno;
	
			return true;
	end;
	$$language plpgsql;
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func cargarFuncion_solicitud_reservas_test(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`	
	create function solicitud_reservas_test() returns void as $$
		declare 
			v record;	
			
		begin
			for v in select * from solicitud_reservas loop 
				select reserva_de_turno(v.nro_paciente, v.dni_medique, v.fecha, v.hora);
			end loop;
	
		end;
	
	$$ language plpgsql;
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func cargarFuncion_cancelar_turnos(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`	
	--Funcion de cancelacion de turnos
	create function cancelar_turnos(dni_medique int, fecha_desde date, fecha_hasta date) returns int as $$
	declare
		cant_turnos_cancelados int;
		v record;
		f_desde timestamp;
		f_hasta timestamp;
		paciente_select paciente%rowtype;
		medique_select medique%rowtype;
	begin
		cant_turnos_cancelados := 0;
		f_desde := fecha_desde + interval '0 hour' + '0 minute' + '0 second';
		f_hasta := fecha_hasta + interval '23 hours' + '59 minutes' + '59 second';
		for v in select * from turno loop
			if v.dni_medique = dni_medique and (v.estado = 'disponible' or v.estado ='reservado') and v.fecha >= f_desde and v.fecha <= f_hasta then
				update turno t set estado = 'cancelado' where t.nro_turno = v.nro_turno; 
				cant_turnos_cancelados = cant_turnos_cancelados + 1;
				select * into paciente_select from paciente p where p.nro_paciente = v.nro_paciente;
				select * into medique_select from medique m where m.dni_medique = v.dni_medique;

				insert into reprogramacion values(v.nro_turno, paciente_select.nombre,paciente_select.apellido,paciente_select.telefono,paciente_select.email, medique_select.nombre, medique_select.apellido, 'pendiente');

			end if;
		end loop;
		return cant_turnos_cancelados;
	end;
	$$ language plpgsql;
	`)
	if err != nil {
		log.Fatal(err)
	}
}
	
func cargarFuncion_atencion_turno(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`	
	--Funcion de atencion de turnos
	create function atencion_turno(num_turno int) returns boolean as $$
	declare
		turno_buscado turno%rowtype;
	begin
		select * into turno_buscado from turno t where t.nro_turno = nro_turno;
		if not found then
			insert into error values(default, null, null, null, null, 'atención', current_timestamp, '?nro de turno no válido');
			return false;
		end if;
		if turno_buscado.estado != 'reservado' then
			insert into error values(default, turno_buscado.fecha, t.nro_consultorio, t.dni_medique, t.nro_paciente, 'atención', current_timestamp, '?turno no reservado');
			return false;
		end if;
		if (date(turno_buscado.fecha)) != (date(now())) then
			insert into error values(default, turno_buscado.fecha, t.nro_consultorio, t.dni_medique, t.nro_paciente, 'atención', current_timestamp, '?turno no corresponde a la fecha del día');
			return false;
		end if;
		update turno t set estado = 'atendido' where t.nro_turno = turno_buscado.nro_turno;
		return true;
	end;
	$$ language plpgsql;
	`)
	if err != nil {
		log.Fatal(err)
	}
}
	
func cargarFuncion_liquidacion_por_obra_social(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`
	create function liquidacion_por_obra_social(año int, mes int, id_obra_social int) returns void as $$
	declare
		el_nro_linea int;
		el_paciente paciente%rowtype;
		el_medique medique%rowtype;
		el_monto_final int;    
		el_turno turno%rowtype;
		el_nro_liquidacion int;
		primera_fecha date;
		ultima_fecha date;
	begin
		el_nro_linea := 1;
		el_monto_final :=0;
		el_nro_liquidacion := obtener_ultima_liquidacion();  
		insert into liquidacion_cabecera values(default, 
		null, null, null, null);
		for el_turno in select * from turno loop
			if extract(year from el_turno.fecha) = año and 
			extract(month from el_turno.fecha) = mes and
			el_turno.estado = 'atendido' and
			el_turno.nro_obra_social_consulta = id_obra_social then		   
				el_paciente := obtener_paciente(el_turno);
				el_medique := obtener_medique(el_turno);
				if el_nro_linea=1 then
					primera_fecha= date(el_turno.fecha);
				end if;
				insert into liquidacion_detalle values(el_nro_liquidacion, nro_liquidacion_detalle,
				el_turno.fecha, el_turno.nro_afiliade_consulta, el_paciente.dni_paciente, 
				el_paciente.nombre, el_paciente.apellido,
				el_turno.dni_medique, el_medique.nombre, el_medique.apellido,
				el_medique.especialidad, el_turno.monto_obra_social);
				update turno t set estado='liquidado' where nro_turno = el_turno.nro_turno;
				el_monto_final = el_monto_final + el_turno.monto_obra_social;
				el_nro_linea = el_nro_linea + 1;
				ultima_fecha= date(el_turno.fecha);
			end if;
		end loop;
		   update liquidacion_cabecera l set nro_obra_social = el_turno.nro_obra_social_consulta,
		   desde = primera_fecha, hasta = ultima_fecha, total = el_monto_final where 
		   nro_liquidacion = el_nro_liquidacion;
	end;
	$$ language plpgsql;

	create function obtener_ultimo_turno(fecha date) returns int as $$
	declare
		el_turno record;
		numero_turno int;
	begin
		for el_turno in select * from turno loop
			numero_turno := el_turno.nro_turno;
		end loop; 
		return numero_turno;
	end;	
	$$ language plpgsql;

	create function crear_cuerpo_mail(turno record, medique record) returns text as $$
	declare 
		texto text;
	begin
		texto := 'Fecha turno: ' || turno.fecha ||
					'. Numero consultorio: ' || turno.nro_consultorio ||
					'. Medique: ' || medique.nombre || ' ' 
					|| medique.apellido || '.';
		return texto;
	end;
	$$ language plpgsql;

	create function obtener_medique(turno record) returns record as $$
	declare
		el_medique record;
	begin
		select * into el_medique from medique where 
		dni_medique = turno.dni_medique;
		return el_medique;
	end;
	$$ language plpgsql;
	
	create function obtener_paciente(turno record) returns record as $$
	declare
		el_paciente record;
	begin
		select * into el_paciente from paciente where 
		nro_paciente = turno.nro_paciente;
		return el_paciente;
	end;
	$$ language plpgsql;
	
	create function obtener_ultima_liquidacion() returns int as $$
	declare
		t record;
		res int;
	begin
		res =1;
		for t in select * from liquidacion_cabecera loop
			res := res + 1;
		end loop; 
		return res;
	end;	
	$$ language plpgsql; 
	`)
	if err != nil {
		log.Fatal(err)
	}
}
	
func cargarFuncion_envio_mail_reservaciones_cancelaciones(){
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	_, err = db.Exec(
	`
	create function envio_email_reservaciones_cancelaciones() returns trigger as $$
	declare
		el_nro_turno int;
		el_turno2 record;
		el_paciente2 record;
		el_medique2 record;
		fecha_hoy date;
		el_cuerpo2 text;
	begin
		fecha_hoy := current_date;
		el_nro_turno := obtener_ultimo_turno(fecha_hoy);
		select * into el_turno2 from turno where nro_turno = el_nro_turno;
		el_paciente2 := obtener_paciente(el_turno2);
		el_medique2 := obtener_medique(el_turno2);
		el_cuerpo2 := crear_cuerpo_mail(el_turno2, el_medique2);
		if el_turno2.estado = 'reservado' then
			insert into envio_email values(default, fecha_hoy, el_paciente2.email,
			'Reserva de turno', el_cuerpo2, fecha_hoy, 'enviado');
		end if;

		if el_turno2.estado = 'cancelado' then
			insert into envio_email values(default, fecha_hoy, el_paciente2.email,
			'Cancelacion de turno', el_cuerpo2, fecha_hoy, 'enviado');
		end if;
	return new;
	end;
	$$ language plpgsql;

create function obtener_ultimo_turno(fecha date) returns int as $$
declare
	el_turno record;
	numero_turno int;
begin
	for el_turno in select * from turno loop
		numero_turno := el_turno.nro_turno;
	end loop;
	return numero_turno;
end;
$$ language plpgsql;

create function crear_cuerpo_mail(turno record, medique record) returns text as $$
declare
	texto text;
begin
	texto := 'Fecha turno: ' || turno.fecha ||
				'. Numero consultorio: ' || turno.nro_consultorio ||
				'. Medique: ' || medique.nombre || ' '
				|| medique.apellido || '.';
	return texto;
end;
$$ language plpgsql;

create function obtener_medique(turno record) returns record as $$
declare
	el_medique record;
begin
	select * into el_medique from medique where
	dni_medique = turno.dni_medique;
	return el_medique;
end;
$$ language plpgsql;

create function obtener_paciente(turno record) returns record as $$
declare
	el_paciente record;
begin
	select * into el_paciente from paciente where
	nro_paciente = turno.nro_paciente;
	return el_paciente;
end;
$$ language plpgsql;

	create trigger envio_email_reservaciones_cancelaciones_trg
	before update of estado on turno
	for each row
	execute procedure envio_email_reservaciones_cancelaciones();
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func completarTablaTurno() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=adm_turnos_medicos sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query
	(
	    `select generar_turnos_disponibles(2023,4);
		 select generar_turnos_disponibles(2023,5);
		 select generar_turnos_disponibles(2023,6);
		 select generar_turnos_disponibles(2023,7);
		 select generar_turnos_disponibles(2023,8);
	`)

	if err != nil{
		log.Fatal(err)
	}

}

func main(){
	i := 1
	for i != 0 {
		
	fmt.Printf("0. Crear base de datos. \n")
	fmt.Printf("1. Crear tablas.\n")
	fmt.Printf("2. Crear Pks & Fks.\n")
	fmt.Printf("3. Completar tablas.\n")
	fmt.Printf("4. Borrar Pks & Fks.\n")
	fmt.Printf("5. Generar turnos.\n")
	fmt.Printf("6. Reservar turnos.\n")
	fmt.Printf("7. Cancelar turnos.\n")
	fmt.Printf("8. Atencion de turnos.\n")
	fmt.Printf("9. Liquidar obra social.\n")
	fmt.Printf("10. Envio de emails.\n")
	fmt.Printf("11. Generar turnos disponibles.\n")
	fmt.Printf("12. Realizar Test.\n")
	fmt.Printf("-1. Salir.\n")
    
	var n int
	fmt.Scanf("%d", &n)
	if n == 0 {
			createDataBase()
			fmt.Printf("\n\nBase de datos creada exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		} else if n == 1 {
			crearTablas()
			fmt.Printf("\n\nTablas creadas exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		} else if n == 2 {
			crearPkFk()
			fmt.Printf("\n\nPks & Fks creadas exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		} else if n == 3 {
			completarTablas()
			fmt.Printf("\n\nTablas completas.\n\n")
			time.Sleep(1 * time.Second)
		} else if n == 4 {
			borrarPkFk()
			fmt.Printf("\n\nPks & Fks borradas exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		} else if n == 5 {
			cargarFuncion_generar_turnos_disponibles()
			fmt.Printf("\n\nFunción cargada exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 6 {
			cargarFuncion_reserva_de_turno()
			fmt.Printf("\n\nFuncion cargada exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 7 {
			cargarFuncion_cancelar_turnos()
			fmt.Printf("\n\nFuncion cargada exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 8 {
			cargarFuncion_atencion_turno()
			fmt.Printf("\n\nFuncion cargada exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 9 {
			cargarFuncion_liquidacion_por_obra_social()
			fmt.Printf("\n\nFuncion cargada exitosamente.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 10 {
			cargarFuncion_envio_mail_reservaciones_cancelaciones()
			fmt.Printf("\n\nFuncion Envio de emails cargada.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 11 {
			completarTablaTurno()
			fmt.Printf("\n\nTurnos disponibles generados.\n\n")
			time.Sleep(1 * time.Second)
		}else if n == 12 {
			cargarFuncion_solicitud_reservas_test()
			fmt.Printf("\n\nTest finalizado.\n\n")
			time.Sleep(1 * time.Second)
		}else {
			i = -1
			fmt.Printf("Adios.\n")
		}		
	i++
	}	
	fmt.Printf("Programa terminado.\n")
}

