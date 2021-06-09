import React from 'react';
import ReactDOM from 'react-dom';
import './index.scss';
import App from './App';
// import { Auth0Provider } from "@auth0/auth0-react";
import * as serviceWorker from './serviceWorker';
import config from './auth0-config.json'
import { Auth0Provider } from '@auth0/auth0-react';
import LoginRedirect  from './components/screens/LogAuth/LoginRedirect';

const auth0enabled = config.auth0_enabled;
// const onRedirectCallback = () => {
//   // If using a Hash Router, you need to use window.history.replaceState to
//   // remove the code and state query parameters from the callback url.
//    window.history.replaceState({}, document.title, window.location.pathname);
//   //history.replace((appState && appState.returnTo) || window.location.pathname);
// };

ReactDOM.render(
  <div>
    {auth0enabled ? (<Auth0Provider
  domain= {config.domain}
  clientId= {config.client_id}
  redirectUri= {window.location.origin}
  audience= {config.audience}
  // onRedirectCallback= {onRedirectCallback}
  // useRefreshTokens= {true}
  // cacheLocation={"localstorage"}
>
  <LoginRedirect/>
  <App />
  </Auth0Provider>) : (<App />)}
    </div>,
document.getElementById('root')
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
