import React, {Component} from 'react';
import Button from '@material-ui/core/Button';
import 'typeface-roboto';
import EventSource from 'eventsource'
import {MuiThemeProvider, createMuiTheme} from '@material-ui/core/styles';
import Card from './Card.js'
import Console from './Console.js'
import TasksTable from './TasksTable.js'
import NetworkSwitch from './NetworkSwitch.js'
import env from './env.js'
import ButtonBar from './ButtonBar.js'
import {withStyles} from '@material-ui/core/styles';
import KelpTalk from './KelpTalk.js'
import tinycolor from 'tinycolor2'

const TButton = withStyles({
  root: {
    margin: '0 8px'
  }
})(Button)

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      response: '',
      open: false,
      kelpTasks: [],
      kelpOffers: [],
      version: '',
      darkMode: false
    }

    this.themeCache = null

    KelpTalk.on('updated', (key, data) => {
      if (key && data) {
        switch (key) {
          case 'version':
            this.setState({version: data})
            break
          case 'kelpTasks':
            this.setState({kelpTasks: data})
            break
          default:
            break
        }
      }
    })

    // fill out the version of kelp
    KelpTalk.get('version')

    this.listen()
  }

  getTheme() {
    if (this.themeCache) {
      return this.themeCache
    }

    const theme = createMuiTheme({
      palette: {
        type: this.state.darkMode
          ? 'dark'
          : 'light'
      },
      typography: {
        // gets rid of deprecation warning, may be removed in the future
        useNextVariants: true
      }
    })

    this.themeCache = theme

    return this.themeCache
  }

  render() {
    const theme = this.getTheme()
    const background = theme.palette.background.default;

    const textColor = tinycolor(theme.palette.text.primary).setAlpha(.5);
    const styles = {
      table: {
        background: "rgba(0,0,0,.7)",
        maxWidth: "900px",
        margin: "20px"
      },
      header: {
        padding: "20px 0",
        fontSize: "8em",
        fontWeight: "bold",
        textTransform: "uppercase",
        lineHeight: 1,
        color: textColor,
        display: "flex",
        flexDirection: "column",
        alignItems: "center"
      },
      subheader: {
        fontSize: ".2em",
        fontWeight: "bold",
        color: "steelblue",
        textTransform: "none"
      },
      cardWrapper: {
        display: 'flex',
        flexDirection: 'column'
      }
    }

    const appStyles = {
      position: "relative",
      zIndex: "2",
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      flex: "1",
      minHeight: "100vh",
      background: background
    }

    const buttons = [
      {
        title: 'New Buysell',
        id: 'buysell'
      }, {
        title: 'New Sell',
        id: 'sell'
      }, {
        title: 'New Balanced',
        id: 'balanced'
      }, {
        title: 'New Mirror',
        id: 'mirror'
      }
    ]

    return (<MuiThemeProvider theme={theme}>
      <div style={appStyles}>
        <NetworkSwitch changed={this.networkChanged}/>
        <header style={styles.header}>
          Kelp
          <div style={styles.subheader}>{this.state.version}</div>
        </header>

        <div>
          <TButton variant="contained" onClick={(e) => this.handleClick('trade')}>
            Trade buy-sell
          </TButton>
          <TButton variant="contained" onClick={(e) => this.handleClick('delete')}>
            Delete
          </TButton>
        </div>

        <div style={styles.cardWrapper}>
          <Card title='Kelp bots' refresh={this.handleTasksRefresh}>
            <TasksTable refresh={this.handleTasksRefresh} tasks={this.state.kelpTasks} handler={this.handleKill}></TasksTable>
            <ButtonBar buttons={buttons} click={this.handleTaskButtons}/>
          </Card>

          <Card title='Console' refresh={() => KelpTalk.clearConsole()}>
            <Console/>
          </Card>
        </div>
      </div>
    </MuiThemeProvider>);
  }

  networkChanged = (value) => {
    this.setState({
      darkMode: value === 'public'
    })
    this.themeCache = null
  }

  listen() {
    var es = new EventSource(env.REST_Url + 'events?stream=messages')

    // we can also use onmessage
    // es.onmessage = (e) => {
    //   if (e.type === 'message') {
    //     console.log('got it', e)
    //   }
    // }

    es.addEventListener('message', (e) => {
      switch (e.data) {
        case 'ping':
          // refresh list
          KelpTalk.get('list')
          break
        default:
          console.log(e.data, 'not handled in switch')
          break
      }
    })

  }

  // binds this for callback without bind(this)
  handleClick = (id) => {
    switch (id) {
      case 'kelp':
        KelpTalk.get('')
        break
      default:
        KelpTalk.get(id)
        break
    }
  }

  // binds this for callback without bind(this)
  handleKill = (process) => {
    if (process.pid && process.pid.length > 0) {
      KelpTalk.put('kill', {Pid: process.pid})
    }
  }

  handleTaskButtons = (id) => {
    console.log(id)
  }

  handleTasksRefresh = () => {
    this.handleClick('list')
  }
}

export default App;