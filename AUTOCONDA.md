# This repo allows to automatically switch to the conda environment when the user navigates to the project directory.

## Quick Start

To set up the conda environment and install all dependencies:

```bash
conda create -n sentinel-canton python=3.10 -y
conda activate sentinel-canton
pip install -e ".[dev]"
```

Then verify the installation:

```bash
pytest -v
```

## Detailed Setup Instructions

To make automatic conda environment switching work, follow these steps:

### 1. Create and Activate the Conda Environment

The environment named in `.condaenv` (`sentinel-canton`) must exist first. Otherwise the hook has
nothing to activate, and `cd` only prints `Auto-deactivating Conda environment: base`.

Create the environment with Python 3.10 (as specified in `pyproject.toml` with `requires-python = ">=3.10"`):

```bash
conda create -n sentinel-canton python=3.10 -y
conda activate sentinel-canton
```

### 2. Install Project Dependencies

The project dependencies are defined in `pyproject.toml`. Install them using pip with the development extras:

```bash
pip install -e ".[dev]"
```

This command:
- Installs the project in editable/development mode (`-e` flag)
- Installs core dependencies: `ext-sentinel-py-sdk`, `aiokafka>=0.11`, `pydantic>=2.6`
- Installs optional `dev` dependencies: `pytest>=8.0`

To install only core dependencies without dev tools, run:

```bash
pip install -e "."
```

To also install the admin API capabilities, run:

```bash
pip install -e ".[admin,dev]"
```

### 3. Configure Shell Hooks for Automatic Activation

If you already have conda, your .bashrc or .zshrc file should have lines like this:

```bash
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/$USER/miniconda3/bin/conda' 'shell.zsh' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/$USER/miniconda3/etc/profile.d/conda.sh" ]; then
        . "/home/$USER/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/$USER/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
```

### 4. Add Auto-activation Hook

To make the autoconda work, add the following lines to the end of your .bashrc or .zshrc file, after conda initialization:

```bash
# Automatic Conda Environment Activation

autoload -U add-zsh-hook

auto_conda_env() {
  local dir="$PWD"
  local prev_env="$CONDA_DEFAULT_ENV"
  local conda_env_name=""
  local found_condaenv_file=""

  while [[ "$dir" != "/" ]]; do
    if [[ -f "$dir/.condaenv" ]]; then
      found_condaenv_file="$dir/.condaenv"
      conda_env_name="$(cat "$found_condaenv_file")"
      break
    fi
    dir="$(dirname "$dir")"
  done

  if [[ -n "$conda_env_name" ]]; then
    if [[ "$CONDA_DEFAULT_ENV" != "$conda_env_name" ]]; then
      if [[ -n "$CONDA_DEFAULT_ENV" ]]; then
        conda deactivate
      fi
      echo "Auto-activating Conda environment: $conda_env_name (from $found_condaenv_file)"
      conda activate "$conda_env_name"
    fi
  else
    if [[ -n "$CONDA_DEFAULT_ENV" ]]; then
      echo "Auto-deactivating Conda environment: $CONDA_DEFAULT_ENV"
      conda deactivate
    fi
  fi
}

add-zsh-hook chpwd auto_conda_env
# Run once when the shell starts
auto_conda_env
```

### 5. Source Your Shell Configuration

Source your .zshrc or .bashrc file:

```bash
source ~/.zshrc
```

### 6. Test Automatic Conda Environment Activation

While in project directory you should see following:
    
```bash
    cd ~/Projects/sentinel-canton                                                                                                        
Auto-activating Conda environment: sentinel-canton (from /home/$USER/Projects/sentinel-canton/.condaenv)
```
And when you echo following variables you should see the following:

```bash
echo $CONDA_DEFAULT_ENV                                                                                                    

sentinel-canton
         
```

or alternatively, you can use the following command to check the active python environment:

```bash
which python3 
```

Output will be path to your conda installation.

## Verifying Your Installation

After setup, verify everything works by running the test suite:

```bash
pytest -v
```

Expected output: All 31 tests should pass.

## Troubleshooting

If the conda environment doesn't activate automatically when you `cd` to the project directory:
1. Verify the conda initialization code is in your `.zshrc` or `.bashrc`
2. Verify the auto-activation hook code is added after conda initialization
3. Run `source ~/.zshrc` (or `.bashrc`) to reload your shell configuration
4. Test with: `cd ~/Projects/sentinel-canton`

## Automatic Deactivation

When you leave the project directory, the conda environment will be deactivated automatically.
For example:

```bash
cd ~
Auto-deactivating Conda environment: sentinel-canton
```

and when you $CONDA_DEFAULT_ENV or check which or where python, you will see your default python installation.
