import React, { useEffect } from "react";
import { useAuth0 } from "@auth0/auth0-react";

const LoginRedirect = () => {
  const { user,isLoading,isAuthenticated, loginWithRedirect, getAccessTokenSilently } = useAuth0();

  if (isLoading) {
    return <></>;
  }

  if (!isAuthenticated) {
    return loginWithRedirect();
  }

  if (isLoading) {
    return <></>;
  }

  useEffect(() => {
      const userIDKey = 'user_id';
      // console.log('the profile ' +JSON.stringify(user));
      const Auth0ID = JSON.stringify(user.sub).slice(7).replace('"', '')
      // console.log('the profile ' +JSON.stringify(Auth0ID));
      localStorage.setItem(userIDKey, Auth0ID);
      // console.log('the profile: ' +JSON.stringify(user.sub).slice(7).replace('"', ''));

      try {
        // const accessToken = await getAccessTokenSilently();
        // localStorage.setItem("accessToken", accessToken);
        getAccessTokenSilently().then(token => localStorage.setItem("accessToken", token)).then(window.location.reload());
      } catch (e) {
        console.log(e.message);
      }
  }, []);

  return (
    <></>
  );
};

export default LoginRedirect;