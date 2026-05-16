package ssh

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
)

// SSHClientConfig contains SSH connection options
type SSHClientConfig struct {
	User           string
	PasswordFile   string
	PrivateKeyFile string
	Port           int
	UseSudo        bool
}

// SSHClient is a minimal SSH client for running commands on a host
type SSHClient struct {
	client *ssh.Client
	cfg    SSHClientConfig
}

// NewSSHClient connects to host with provided config. Host may include port; port in cfg overrides when non-zero.
func NewSSHClient(host string, cfg SSHClientConfig) (*SSHClient, error) {
	var authMethods []ssh.AuthMethod

	if cfg.PrivateKeyFile != "" {
		key, err := os.ReadFile(cfg.PrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read private key file: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	} else if cfg.PasswordFile != "" {
		p, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return nil, fmt.Errorf("read password file: %w", err)
		}

		password := string(p)
		authMethods = append(authMethods, ssh.Password(password))
	} else {
		// no auth provided
		return nil, fmt.Errorf("no ssh auth method provided")
	}

	port := cfg.Port
	if port == 0 {
		port = 22
	}

	sshCfg := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := ssh.Dial("tcp", addr, sshCfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}

	return &SSHClient{client: c, cfg: cfg}, nil
}

// Run runs a command on the remote host and returns stdout, stderr and error.
func (s *SSHClient) Run(ctx context.Context, cmd string) (string, string, error) {
	session, err := s.client.NewSession()
	if err != nil {
		return "", "", err
	}
	defer session.Close()

	if s.cfg.UseSudo {
		cmd = "sudo " + cmd
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	stderr, err := session.StderrPipe()
	if err != nil {
		return "", "", err
	}

	if err := session.Start(cmd); err != nil {
		return "", "", err
	}

	outBytes, _ := io.ReadAll(stdout)
	errBytes, _ := io.ReadAll(stderr)

	if err := session.Wait(); err != nil {
		// return output and error
		return string(outBytes), string(errBytes), err
	}

	return string(outBytes), string(errBytes), nil
}
