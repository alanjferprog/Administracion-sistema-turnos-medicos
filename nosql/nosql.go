package main

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"strconv"
	"time"
)

/* Crear buckets */

type Paciente struct {
	NroPaciente int
    Nombre string
    Apellido string
	DniPaciente int
    FNac time.Date
	NroObraSocial int
    NroAfiliade int
    Domicilio string
    Telefono string 
    Mail string
}

type Medique struct {
    DniMedique int
    Nombre string
    Apellido string
    Especialidad string
	MontoConsultaPrivada float64
    Telefono string
}

type Consultorio struct {
    NroConsultorio int
    Nombre string
    Domicilio string
	CodigolPostal string
    Telefono string
}

type Turno struct {
	NroTurno int
	Fecha time.Time
	NroConsultorio int
	DniMedique int
	NroPaciente int
	NroObraSocialConsulta int
	NroAfiliadeConsulta int
	MontoPaciente float64
	MontoObraSocial float64
	FReserva time.Time
	Estado string
}

type Obra_social struct {
	NroObraSocial int
	Nombre string
	ContactoNombre string
	ContactoApellido string
	ContactoTelefono string
	ContactoEmail string
}

/* Escribir buckets */

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))

	err = b.Put(key, val)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

/* Leer buckets */

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	})

	return buf, err
}

/* Carga de pacientes */

func CargarPacientes(db *bolt.DB){
	pacienteUno := Paciente{1,"Amparo", "Carranza", 35297524, "1992-05-07", 701125 , 7524, "Carlos Pellegrini 451", "11-6409-5604", "acarranza92@gmail.com"}
	pacienteDos := Paciente{2, "Zahra", "Bernabeu", 43620390, "2001-08-26", 003153 , 0390, "Cuenca 3384", "11-2462-2971", "zbernadeu01@gmail.com"}
	pacienteTres := Paciente{3, "Elizabeth", "Paredes", 19830720, "1973-10-25", 301070 , 0720, "Córdoba 1765", "11-431-5272", "eparedes73@gmail.com"}
	
	dataUno, err := json.Marshal(pacienteUno)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"pacienteUno", []byte(strconv.Itoa(pacienteUno.NroPaciente)), dataUno)
	
	resultadoUno, err := ReadUnique(db,"pacienteUno", []byte(strconv.Itoa(pacienteUno.NroPaciente)))
	
	fmt.Printf("\nPacientes:\n")
	fmt.Printf("%s\n", resultadoUno)
	
	dataDos, err := json.Marshal(pacienteDos)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"pacienteDos", []byte(strconv.Itoa(pacienteDos.NroPaciente)), dataDos)
	
	resultadoDos, err := ReadUnique(db,"pacienteDos", []byte(strconv.Itoa(pacienteDos.NroPaciente)))
	
	fmt.Printf("%s\n", resultadoDos)
	
	dataTres, err := json.Marshal(pacienteTres)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"pacienteTres", []byte(strconv.Itoa(pacienteTres.NroPaciente)), dataTres)
	
	resultadoTres, err := ReadUnique(db,"pacienteTres", []byte(strconv.Itoa(pacienteTres.NroPaciente)))
	
	fmt.Printf("%s\n", resultadoTres)	
}

/* Carga de mediques */

func CargarMediques(db *bolt.DB){
	mediqueUno := Medique{35428517, "Julian", "Gonzalez", "cardiologia", 5000.00, "11-3257-4789"}
	mediqueDos := Medique{20250305, "Gabriela", "Paez", "", 2500.00, "11-7766-2521"}
	mediqueTres := Medique{27879111, "Lautaro", "Sabatte", "", 2500.00, "11-5596-5824"}
	
	dataUno, err := json.Marshal(mediqueUno)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"mediqueUno", []byte(strconv.Itoa(mediqueUno.DniMedique)), dataUno)
	
	resultadoUno, err := ReadUnique(db,"mediqueUno", []byte(strconv.Itoa(mediqueUno.DniMedique)))
	
	fmt.Printf("\nMediques:\n")
	fmt.Printf("%s\n", resultadoUno)
	
	dataDos, err := json.Marshal(mediqueDos)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"mediqueDos", []byte(strconv.Itoa(mediqueDos.DniMedique)), dataDos)
	
	resultadoDos, err := ReadUnique(db,"mediqueDos", []byte(strconv.Itoa(mediqueDos.DniMedique)))
	
	fmt.Printf("%s\n", resultadoDos)
	
	dataTres, err := json.Marshal(mediqueTres)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"mediqueTres", []byte(strconv.Itoa(mediqueTres.DniMedique)), dataTres)
	
	resultadoTres, err := ReadUnique(db,"mediqueTres", []byte(strconv.Itoa(mediqueTres.DniMedique)))
	
	fmt.Printf("%s\n", resultadoTres)	
}

/* Carga de consultorios */

func CargarConsultorios(db *bolt.DB){
	consultorioUno := Consultorio{1, "Sarmiento", "Peron 1207", "B1663SAN", "11-4455-7900"}
	consultorioDos := Consultorio{2, "Roca", "Arias 2369", "B1712MOR", "11-4455-8000"}
	consultorioTres := Consultorio{3, "Bessone", "Libertador 487", "B1722MER", "11-4455-8100"}
	
	dataUno, err := json.Marshal(consultorioUno)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"consultorioUno", []byte(strconv.Itoa(consultorioUno.NroConsultorio)), dataUno)
	
	resultadoUno, err := ReadUnique(db,"consultorioUno", []byte(strconv.Itoa(consultorioUno.NroConsultorio)))
	
	fmt.Printf("\nConsultorios:\n")
	fmt.Printf("%s\n", resultadoUno)
	
	dataDos, err := json.Marshal(consultorioDos)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"consultorioDos", []byte(strconv.Itoa(consultorioDos.NroConsultorio)), dataDos)
	
	resultadoDos, err := ReadUnique(db,"consultorioDos", []byte(strconv.Itoa(consultorioDos.NroConsultorio)))
	
	fmt.Printf("%s\n", resultadoDos)
	
	dataTres, err := json.Marshal(consultorioTres)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"consultorioTres", []byte(strconv.Itoa(consultorioTres.NroConsultorio)), dataTres)
	
	resultadoTres, err := ReadUnique(db,"consultorioTres", []byte(strconv.Itoa(consultorioTres.NroConsultorio)))
	
	fmt.Printf("%s\n", resultadoTres)	
}

/* Carga de obras sociales */

func CargarObrasSociales(db *bolt.DB){
	obraSocialUno := Obra_social{701125, "OSECAC", "Juliana", "Rodriguez", "11-0022-4789", "info@osecac.org.ar"}
	obraSocialDos := Obra_social{003153, "OSDOP", "Gustavo", "Gian", "11-0033-2521","info@osdop.org.ar"}
	obraSocialTres := Obra_social{301070, "IOMA", "Mariana", "Cabezas", "11-0044-5824","info@ioma.org.ar"}
	
	dataUno, err := json.Marshal(obraSocialUno)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"obraSocialUno", []byte(strconv.Itoa(obraSocialUno.NroObraSocial)), dataUno)
	
	resultadoUno, err := ReadUnique(db,"obraSocialUno", []byte(strconv.Itoa(obraSocialUno.NroObraSocial)))
	
	fmt.Printf("\nObras sociales:\n")
	fmt.Printf("%s\n", resultadoUno)
	
	dataDos, err := json.Marshal(obraSocialDos)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"obraSocialDos", []byte(strconv.Itoa(obraSocialDos.NroObraSocial)), dataDos)
	
	resultadoDos, err := ReadUnique(db,"obraSocialDos", []byte(strconv.Itoa(obraSocialDos.NroObraSocial)))
	
	fmt.Printf("%s\n", resultadoDos)
	
	dataTres, err := json.Marshal(obraSocialTres)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"obraSocialTres", []byte(strconv.Itoa(obraSocialTres.NroObraSocial)), dataTres)
	
	resultadoTres, err := ReadUnique(db,"obraSocialTres", []byte(strconv.Itoa(obraSocialTres.NroObraSocial)))
	
	fmt.Printf("%s\n", resultadoTres)	
}

/* Carga de turnos */

func CargarTurnos(db *bolt.DB) {
	turnoUno := Turno{1, "2023-06-12 9:00:00", 1, 35428517, 1, 701125, 7524, 1000.00, 1500.00, "2023-05-10 12:03:00", "atendido"}
	turnoDos := Turno{2, "2023-07-10 14:15:00", 1, 35428517, 1, 701125, 7524, 1000.00, 1500.00, "2023-06-01 10:25:00", "reservado"}
	turnoTres := Turno{3, "2023-08-14 9:45:00", 1, 35428517, 1, 701125, 7524, 1000.00, 1500.00, "2023-07-20 15:45:00", "reservado"}
	
	dataUno, err := json.Marshal(turnoUno)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoUno", []byte(strconv.Itoa(turnoUno.NroTurno)), dataUno)
	
	resultadoUno, err := ReadUnique(db,"turnoUno", []byte(strconv.Itoa(turnoUno.NroTurno)))
	
	fmt.Printf("\nTurnos:\n")
	fmt.Printf("%s\n", resultadoUno)
	
	dataDos, err := json.Marshal(turnoDos)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoDos", []byte(strconv.Itoa(turnoDos.NroTurno)), dataDos)
	
	resultadoDos, err := ReadUnique(db,"turnoDos", []byte(strconv.Itoa(turnoDos.NroTurno)))
	
	fmt.Printf("%s\n", resultadoDos)
	
	dataTres, err := json.Marshal(turnoTres)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoTres", []byte(strconv.Itoa(turnoTres.NroTurno)), dataTres)
	
	resultadoTres, err := ReadUnique(db,"turnoTres", []byte(strconv.Itoa(turnoTres.NroTurno)))
	
	fmt.Printf("%s\n", resultadoTres)
	
	turnoCuatro := Turno{4, "2023-06-06 08:30:00", 1, 20250305, 2, 003153, 0390, 700.00, 1700.00, "2023-05-25 16:32:00", "atendido"}
	turnoCinco := Turno{5, "2023-06-13 10:15:00", 1, 20250305, 2, 003153, 0390, 700.00, 1700.00, "2023-06-01 11:12:00", "cancelado"}
	turnoSeis := Turno{6, "2023-07-04 11:45:00", 1, 20250305, 2, 003153, 0390, 700.00, 1700.00, "2023-06-28 14:20:00", "reservado"}
	
	dataCuatro, err := json.Marshal(turnoCuatro)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoCuatro", []byte(strconv.Itoa(turnoCuatro.NroTurno)), dataCuatro)
	
	resultadoCuatro, err := ReadUnique(db,"turnoCuatro", []byte(strconv.Itoa(turnoCuatro.NroTurno)))
	
	fmt.Printf("%s\n", resultadoCuatro)
	
	dataCinco, err := json.Marshal(turnoCinco)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoCinco", []byte(strconv.Itoa(turnoCinco.NroTurno)), dataCinco)
	
	resultadoCinco, err := ReadUnique(db,"turnoCinco", []byte(strconv.Itoa(turnoCinco.NroTurno)))
	
	fmt.Printf("%s\n", resultadoCinco)
	
	dataSeis, err := json.Marshal(turnoSeis)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoSeis", []byte(strconv.Itoa(turnoSeis.NroTurno)), dataSeis)
	
	resultadoSeis, err := ReadUnique(db,"turnoSeis", []byte(strconv.Itoa(turnoSeis.NroTurno)))
	
	fmt.Printf("%s\n", resultadoSeis)
	
	turnoSiete := Turno{7, "2023-05-17 08:40:00", 1, 27879111, 3, 301070, 0720, 500.00, 1700.00, "2023-04-16 15:23:00", "liquidado"}
	turnoOcho := Turno{8, "2023-07-05 09:20:00", 1, 27879111, 3, 301070, 0720, 500.00, 1700.00, "2023-06-24 17:46:00", "reservado"}
	turnoNueve := Turno{9, "2023-08-16 11:20:00", 1, 27879111, 3, 301070, 0720, 500.00, 1700.00, "2023-08-02 11:02:00", "reservado"}
	
	dataSiete, err := json.Marshal(turnoSiete)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoSiete", []byte(strconv.Itoa(turnoSiete.NroTurno)), dataSiete)
	
	resultadoSiete, err := ReadUnique(db,"turnoSiete", []byte(strconv.Itoa(turnoSiete.NroTurno)))
	
	fmt.Printf("%s\n", resultadoSiete)
	
	dataOcho, err := json.Marshal(turnoOcho)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoOcho", []byte(strconv.Itoa(turnoOcho.NroTurno)), dataOcho)
	
	resultadoOcho, err := ReadUnique(db,"turnoOcho", []byte(strconv.Itoa(turnoOcho.NroTurno)))
	
	fmt.Printf("%s\n", resultadoOcho)
	
	dataNueve, err := json.Marshal(turnoNueve)
	if err != nil{
		log.Fatal(err)
	}
	CreateUpdate(db,"turnoNueve", []byte(strconv.Itoa(turnoNueve.NroTurno)), dataNueve)
	
	resultadoNueve, err := ReadUnique(db,"turnoNueve", []byte(strconv.Itoa(turnoNueve.NroTurno)))
	
	fmt.Printf("%s\n", resultadoNueve)
}
