package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/pprof/profile"
	"github.com/lib/pq"
	"github.com/lib/pq/hstore"
	"github.com/pkg/errors"
)

type DBConfig struct {
	Host     string
	User     string
	Password string
	Database string
}

func (conf DBConfig) ConnString() string {
	return fmt.Sprintf(
		"user=%s password=%s host=%s dbname=%s sslmode=disable",
		conf.User,
		conf.Password,
		conf.Host,
		conf.Database,
	)
}

func (conf *DBConfig) RegisterFlags(fg *flag.FlagSet) {
	fg.StringVar(&conf.Host, "pg.host", "localhost", "db host")
	fg.StringVar(&conf.User, "pg.user", "postgres", "db user")
	fg.StringVar(&conf.Password, "pg.password", "postgres", "db password")
	fg.StringVar(&conf.Database, "pg.database", "pprof_data", "db name")
}

func main() {
	var dbConf DBConfig

	fg := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	dbConf.RegisterFlags(fg)

	if err := fg.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	log.SetOutput(os.Stdout)

	if len(fg.Args()) == 0 {
		log.Fatal("no profiles passed")
	}

	if err := run(dbConf, fg.Args()...); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run(conf DBConfig, files ...string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", conf.ConnString())
	if err != nil {
		return errors.Wrap(err, "could not open db")
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return errors.Wrap(err, "could not ping db")
	}

	meta := map[string]string{
		"build_id": "456",
		"token":    "fra.1",
		"service":  "adjust_server",
		"dc":       "fra",
		"host":     "backend-1",
	}

	storage := NewProfileStorage(db)

	for _, f := range files {
		if err := storage.CreateProfile(ctx, meta, f); err != nil {
			return err
		}
	}

	return nil
}

type Profile struct {
	prof *profile.Profile

	BuildID    string
	Token      string
	Service    string
	CreatedAt  time.Time
	ReceivedAt time.Time
	Labels     map[string]string
}

func (p *Profile) Parse(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	prof, err := profile.Parse(f)
	if err != nil {
		return err
	}

	p.prof = prof

	return nil
}

type ProfileStorage struct {
	db *sql.DB
}

func NewProfileStorage(db *sql.DB) *ProfileStorage {
	return &ProfileStorage{db}
}

const (
	sqlInsertServices = `INSERT INTO services(build_id, token, name, labels) VALUES ($1, $2, $3, $4) ON CONFLICT (build_id, token) DO NOTHING;`

	sqlInsertLocations = `
		INSERT INTO profile_pprof_locations (func, file_name, line) 
		SELECT tmp.func, tmp.file_name, tmp.line 
		FROM profile_pprof_samples_tmp AS tmp ON CONFLICT DO NOTHING;`
	sqlInsertSamples = `
		INSERT INTO profile_pprof_samples_cpu (build_id, token, locations, created_at, value_cpu, value_nanos)
		SELECT s.build_id, s.token, t.locations, s.created_at, t.value_cpu, t.value_nanos 
		FROM (values ($1, $2, $3::timestamp)) as s (build_id, token, created_at),
	  	(
			SELECT sample_id, array_agg(l.location_id) as locations, value_cpu, value_nanos
			FROM profile_pprof_samples_tmp tmp
			INNER JOIN profile_pprof_locations l ON tmp.func = l.func AND tmp.file_name = l.file_name AND tmp.line = l.line
			GROUP BY sample_id, value_cpu, value_nanos
		) as t;`
)

const sqlCreateTempTable = `CREATE TEMPORARY TABLE IF NOT EXISTS profile_pprof_samples_tmp (sample_id INTEGER, location_id INTEGER, func TEXT, file_name TEXT, line INT, value_cpu INTEGER, value_nanos INTEGER) ON COMMIT DELETE ROWS;`
var sqlCopyTable = pq.CopyIn("profile_pprof_samples_tmp", "sample_id", "location_id", "func", "file_name", "line", "value_cpu", "value_nanos")

func (s *ProfileStorage) CreateProfile(ctx context.Context, meta map[string]string, filePath string) error {
	prof, err := s.createProfile(meta, filePath)
	if err != nil {
		return errors.Wrapf(err, "could not parse profile %q", filePath)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		sqlInsertServices,
		prof.BuildID,
		prof.Token,
		prof.Service,
		hstoreFromLabels(prof.Labels),
	)
	if err != nil {
		return errors.Wrap(err, "could not INSERT service")
	}

	_, err = tx.ExecContext(ctx, sqlCreateTempTable)
	if err != nil {
		return errors.Wrapf(err, "could not create temp table %q", sqlCreateTempTable)
	}

	copyStmt, err := tx.PrepareContext(ctx, sqlCopyTable)
	if err != nil {
		return errors.Wrapf(err, "could not prepare COPY statement %q", sqlCopyTable)
	}

	for sampleID, sample := range prof.prof.Sample {
		for locID, loc := range sample.Location {
			for _, ln := range loc.Line {
				_, err := copyStmt.ExecContext(
					ctx,
					sampleID,
					locID,
					ln.Function.Name,
					ln.Function.Filename,
					ln.Line,
					sample.Value[0],
					sample.Value[1],
				)
				if err != nil {
					return errors.Wrap(err, "could not exec COPY statement")
				}
			}
		}
	}

	_, err = copyStmt.ExecContext(ctx)
	if err != nil {
		return errors.Wrap(err, "could not exec COPY statement")
	}

	_, err = tx.ExecContext(ctx, sqlInsertLocations)
	if err != nil {
		return errors.Wrap(err, "could not insert locations")
	}

	_, err = tx.ExecContext(
		ctx,
		sqlInsertSamples,
		prof.BuildID,
		prof.Token,
		prof.CreatedAt,
	)
	if err != nil {
		return errors.Wrap(err, "could not insert samples")
	}

	if err := copyStmt.Close(); err != nil {
		return errors.Wrap(err, "could not close COPY statement")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "could not commit transaction")
	}

	return nil
}

func (s *ProfileStorage) createProfile(meta map[string]string, filePath string) (*Profile, error) {
	prof := &Profile{}
	if err := prof.Parse(filePath); err != nil {
		return nil, err
	}

	if prof.prof.TimeNanos != 0 {
		prof.CreatedAt = time.Unix(0, prof.prof.TimeNanos)
	}

	for k, v := range meta {
		switch k {
		case "build_id":
			prof.BuildID = v
		case "token":
			prof.Token = v
		case "service":
			prof.Service = v
		case "received_at":
			tm, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, err
			}
			prof.ReceivedAt = tm
		default:
			if prof.Labels == nil {
				prof.Labels = make(map[string]string)
			}
			prof.Labels[k] = v
		}
	}

	return prof, nil
}

func hstoreFromLabels(labels map[string]string) hstore.Hstore {
	hs := hstore.Hstore{
		Map: make(map[string]sql.NullString, len(labels)),
	}
	for key, value := range labels {
		hs.Map[key] = sql.NullString{String: value, Valid: true}
	}
	return hs
}
